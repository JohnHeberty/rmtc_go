import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pyarrow.dataset as ds
from requests import HTTPError, RequestException, Session

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import (
    BRONZE,
    SILVER,
    list_files,
    partition_dir,
    read_json,
    write_json_new,
)

from .config import DATASET, SENTIDOS, SOURCE_DATASET
from .schemas import FetchEntry, FetchLog, KmlBronzeRecord, KmlMetadata

logger = get_logger(__name__)

# Linha, sentido e arquivo viram nome de pasta: só caracteres seguros
SAFE = re.compile(r"^[A-Za-z0-9_-]+$")


class ExtractError(RuntimeError):
    """A RMTC respondeu, mas não com um KML."""


@dataclass(frozen=True, order=True)
class Target:
    linha: str
    sentido: str
    url: str  # sem query string

    @property
    def arquivo(self) -> str:
        """Nome do KML sem extensão. Uma linha pode ter mais de um por sentido (um por eixo,
        ex.: Eixo85-004ida e EixoT9-004ida), então ele também identifica o traçado."""
        return Path(urlsplit(self.url).path).stem


@dataclass(frozen=True)
class Kml:
    text: str
    last_modified: datetime | None = None
    etag: str | None = None


@dataclass
class ExtractResult:
    targets: int = 0
    saved: list[Path] = field(default_factory=list)
    unchanged: int = 0
    failed: list[str] = field(default_factory=list)
    entries: list[FetchEntry] = field(default_factory=list)


def strip_query(url: str) -> str:
    """Remove o ?rev=YYYYMMDD-HH: muda a cada hora, mas o arquivo é o mesmo."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def targets_of_day(day: date, settings: Settings) -> set[Target]:
    """Linhas/sentidos e URLs de KML vistos no silver de posições de um dia."""
    files = list_files(partition_dir(settings.data_dir, SILVER, SOURCE_DATASET, day), "*.parquet")
    if not files:
        return set()

    table = ds.dataset(files, format="parquet").to_table(columns=["LinhaNumero", *SENTIDOS])
    targets: set[Target] = set()
    for column, sentido in SENTIDOS.items():
        pairs = (
            table.select(["LinhaNumero", column]).group_by(["LinhaNumero", column]).aggregate([])
        )
        for linha, url in zip(*(pairs.column(i).to_pylist() for i in range(2)), strict=True):
            if not url or not linha:
                continue  # KML null (ex.: falha da RMTC das 04h em 28/10)
            target = Target(linha=linha, sentido=sentido, url=strip_query(url))
            if not (SAFE.match(linha) and SAFE.match(target.arquivo)):
                logger.warning("KML com nome inesperado ignorado: %r %r", linha, target.url)
                continue
            targets.add(target)
    return targets


def discover_targets(days: list[date], settings: Settings) -> list[Target]:
    """Traçados usados pelas posições nesses dias.

    Duas URLs que caem na mesma pasta do bronze (mesmo nome de arquivo em caminhos
    diferentes) misturariam o histórico: fica só a primeira, com aviso.
    """
    targets: dict[tuple[str, str, str], Target] = {}
    for target in sorted(set().union(*(targets_of_day(day, settings) for day in days))):
        key = (target.linha, target.sentido, target.arquivo)
        if (kept := targets.setdefault(key, target)) != target:
            logger.warning("KML %s ignorado: mesma pasta do bronze que %s", target.url, kept.url)
    return sorted(targets.values())


def bronze_root(settings: Settings) -> Path:
    return settings.data_dir / BRONZE / DATASET


def target_dir(target: Target, settings: Settings) -> Path:
    """Histórico de versões de um traçado: bronze/linha_kml/linha=/sentido=/arquivo=/."""
    return (
        bronze_root(settings)
        / f"linha={target.linha}"
        / f"sentido={target.sentido}"
        / f"arquivo={target.arquivo}"
    )


def fetch_dir(settings: Settings) -> Path:
    return bronze_root(settings) / "_fetch"


def version_files(directory: Path) -> list[Path]:
    return list_files(directory, "*.json.gz")


def latest_hash(target: Target, settings: Settings) -> str | None:
    """Hash da versão legível mais recente do traçado.

    Pula arquivos ilegíveis: se o último estiver corrompido, compara com o anterior legível,
    em vez de gravar de novo uma versão que o dedup apagaria (o que se repetiria todo dia).
    """
    for path in reversed(version_files(target_dir(target, settings))):
        try:
            return content_hash(read_json(path)["payload"])
        except ValueError, OSError, EOFError, KeyError, TypeError, AttributeError:
            continue
    return None


def http_date(value: str | None) -> datetime | None:
    """Data de um cabeçalho HTTP (None se faltar ou não der para ler)."""
    try:
        parsed = parsedate_to_datetime(value) if value else None
    except TypeError, ValueError:
        return None
    return parsed if parsed and parsed.tzinfo else None


def fetch_kml(session: Session, url: str, timeout: float = 30) -> Kml:
    response = session.get(url, timeout=timeout)
    try:
        response.raise_for_status()
    except HTTPError as e:
        raise ExtractError(f"RMTC respondeu {response.status_code} para {url}") from e
    text = response.content.decode("utf-8")
    if "<kml" not in text[:2000]:
        raise ExtractError(f"Resposta de {url} não é KML: {text[:200]!r}")
    return Kml(
        text=text,
        last_modified=http_date(response.headers.get("Last-Modified")),
        etag=response.headers.get("ETag"),
    )


def save_version(
    target: Target,
    kml: str,
    collected_at: datetime,
    settings: Settings,
    last_modified: datetime | None = None,
    etag: str | None = None,
) -> Path:
    """Grava uma versão nova do traçado (nunca sobrescreve)."""
    if collected_at.tzinfo is None:
        raise ValueError("collected_at precisa ter timezone")
    record = KmlBronzeRecord(
        metadata=KmlMetadata(
            dataset=DATASET,
            linha=target.linha,
            sentido=target.sentido,
            source_url=target.url,
            collected_at=collected_at,
            payload_sha256=content_hash(kml),
            last_modified=last_modified,
            etag=etag,
        ),
        payload=kml,
    )
    path = target_dir(target, settings) / f"{collected_at:%Y%m%dT%H%M%S}.json.gz"
    return write_json_new(record.model_dump(mode="json"), path)


def extract_targets(
    session: Session,
    targets: list[Target],
    settings: Settings,
    delay: float = 0,
) -> ExtractResult:
    """Baixa cada KML e grava só os que mudaram. Uma falha não interrompe os demais.

    Toda tentativa vira uma FetchEntry (com ou sem mudança, com ou sem falha).
    """
    result = ExtractResult(targets=len(targets))
    for i, target in enumerate(targets):
        if i and delay:
            time.sleep(delay)
        entry = {
            "linha": target.linha,
            "sentido": target.sentido,
            "arquivo": target.arquivo,
            "url": target.url,
            "fetched_at": datetime.now(settings.tz),
        }
        try:
            kml = fetch_kml(session, target.url, settings.http_timeout)
        except (ExtractError, RequestException, UnicodeDecodeError) as e:
            logger.warning("KML %s/%s não baixado: %s", target.linha, target.sentido, e)
            result.failed.append(f"{target.linha}/{target.sentido}/{target.arquivo}")
            result.entries.append(FetchEntry(**entry, status="erro", erro=str(e)[:500]))
            continue

        entry |= {"last_modified": kml.last_modified, "etag": kml.etag}
        sha256 = content_hash(kml.text)
        saved = None
        if sha256 == latest_hash(target, settings):
            result.unchanged += 1
        else:
            path = save_version(
                target, kml.text, entry["fetched_at"], settings, kml.last_modified, kml.etag
            )
            result.saved.append(path)
            saved = path.relative_to(bronze_root(settings)).as_posix()
        result.entries.append(FetchEntry(**entry, status="ok", sha256=sha256, salvo=saved))
    return result


def save_fetch_log(
    entries: list[FetchEntry], days: list[date], started_at: datetime, settings: Settings
) -> Path:
    """Registro da execução (um arquivo novo por execução, nunca sobrescrito)."""
    log = FetchLog(
        dataset=DATASET,
        started_at=started_at,
        window=[days[0].isoformat(), days[-1].isoformat()],
        entries=entries,
    )
    path = fetch_dir(settings) / f"{started_at:%Y%m%dT%H%M%S}.json.gz"
    return write_json_new(log.model_dump(mode="json"), path)
