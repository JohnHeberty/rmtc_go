"""Bronze -> silver dos traçados: kml_observacao (cada verificação) e kml_versao (versões).

Os dois são reconstruídos inteiros a partir do bronze a cada execução (são pequenos), com
escrita atômica e recusa se o resultado tiver menos linhas que o silver atual.
"""

import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import (
    QUARANTINE,
    SILVER,
    list_files,
    read_json,
    write_json,
    write_parquet,
)

from .config import DATASET
from .extract import bronze_root, content_hash, fetch_dir, version_files
from .schemas import FetchLog, KmlBronzeRecord, observacao_schema, versao_schema

logger = get_logger(__name__)

KML_NS = "{http://www.opengis.net/kml/2.2}"


class SilverShrinkError(RuntimeError):
    """O silver reconstruído teria menos linhas que o atual: o bronze perdeu arquivos?"""


@dataclass(frozen=True)
class LoadResult:
    observacoes: int
    versoes: int
    rejected: int
    path_observacao: Path
    path_versao: Path


@dataclass(frozen=True)
class Parsed:
    nome: str | None
    geometria: str
    pontos: int
    hash_geometria: str


def silver_dir(settings: Settings) -> Path:
    return settings.data_dir / SILVER / DATASET


def quarantine_path(settings: Settings) -> Path:
    return settings.data_dir / QUARANTINE / DATASET / "rejected.json"


def parse_kml(kml: str) -> tuple[str | None, str, int]:
    """Extrai (nome, geometria WKT, nº de pontos) das LineString do KML."""
    root = ET.fromstring(kml)
    lines: list[list[str]] = []
    for coords in root.iter(f"{KML_NS}LineString"):
        text = coords.findtext(f"{KML_NS}coordinates") or ""
        points = []
        for item in text.split():
            lon, lat, *_ = item.split(",")
            points.append(f"{float(lon)} {float(lat)}")
        if len(points) >= 2:
            lines.append(points)
    if not lines:
        raise ValueError("KML sem LineString com pelo menos 2 pontos")

    name = next(
        (
            n.strip()
            for pm in root.iter(f"{KML_NS}Placemark")
            if (n := pm.findtext(f"{KML_NS}name"))
        ),
        None,
    )
    if len(lines) == 1:
        wkt = f"LINESTRING ({', '.join(lines[0])})"
    else:
        wkt = "MULTILINESTRING (" + ", ".join(f"({', '.join(p)})" for p in lines) + ")"
    return name, wkt, sum(len(p) for p in lines)


def geometry_hash(wkt: str) -> str:
    """Hash da geometria normalizada: muda só se o traçado mudar, não a formatação do KML."""
    return hashlib.sha256(wkt.encode()).hexdigest()


@dataclass
class Bronze:
    parsed: dict[str, Parsed] = field(default_factory=dict)  # sha256 do KML bruto -> geometria
    # sha256 -> Last-Modified gravado com a versão (reserva para registros _fetch sem ele)
    modified: dict[str, datetime] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)


def read_bronze(settings: Settings) -> Bronze:
    """Lê versões e registros de execução do bronze.

    Observações vêm dos registros _fetch; versões gravadas antes de existir o registro (ou
    sem registro legível) entram como uma observação da própria versão.
    """
    root = bronze_root(settings)
    out = Bronze()

    logged: set[str] = set()  # versões já citadas em algum _fetch
    for path in list_files(fetch_dir(settings), "*.json.gz"):
        try:
            log = FetchLog.model_validate(read_json(path))
        except (ValueError, OSError, EOFError, ValidationError) as e:
            out.rejected.append({"file": path.relative_to(root).as_posix(), "error": str(e)})
            continue
        for e in log.entries:
            out.observations.append(
                {
                    "Url": e.url,
                    "Arquivo": f"{e.arquivo}.kml",
                    "LinhaNumero": e.linha,
                    "Sentido": e.sentido,
                    "ObservadoEm": e.fetched_at.astimezone(settings.tz),
                    "Status": e.status,
                    "Erro": e.erro,
                    "Sha256": e.sha256,
                    "ModificadoEm": e.last_modified,
                    "Origem": "fetch",
                }
            )
            if e.salvo:
                logged.add(e.salvo)

    for directory in sorted(root.glob("linha=*/sentido=*/arquivo=*")):
        for path in version_files(directory):
            ref = path.relative_to(root).as_posix()
            try:
                record = KmlBronzeRecord.model_validate(read_json(path))
                name, wkt, n_points = parse_kml(record.payload)
            except (ValueError, OSError, EOFError, ET.ParseError, ValidationError) as e:
                out.rejected.append({"file": ref, "error": str(e)})
                continue
            sha256 = content_hash(record.payload)  # recalculado, não confia no metadata
            out.parsed[sha256] = Parsed(name, wkt, n_points, geometry_hash(wkt))
            meta = record.metadata
            if meta.last_modified:
                out.modified.setdefault(sha256, meta.last_modified)
            if ref not in logged:
                out.observations.append(
                    {
                        "Url": meta.source_url,
                        "Arquivo": Path(meta.source_url).name,
                        "LinhaNumero": meta.linha,
                        "Sentido": meta.sentido,
                        "ObservadoEm": meta.collected_at.astimezone(settings.tz),
                        "Status": "ok",
                        "Erro": None,
                        "Sha256": sha256,
                        "ModificadoEm": meta.last_modified,
                        "Origem": "versao",
                    }
                )

    for obs in out.observations:
        parsed = out.parsed.get(obs["Sha256"]) if obs["Sha256"] else None
        obs["HashGeometria"] = parsed.hash_geometria if parsed else None
        modified = obs["ModificadoEm"] or out.modified.get(obs["Sha256"])
        obs["ModificadoEm"] = modified.astimezone(settings.tz) if modified else None
    out.observations.sort(key=lambda o: (o["Url"], o["ObservadoEm"]))
    return out


def build_versions(bronze: Bronze) -> list[dict[str, Any]]:
    """Blocos seguidos de observações ok com a mesma geometria, por arquivo (URL).

    A, B, A são três versões (a rota voltou: é mudança real). Falhas de download não
    quebram um bloco, mas também não o confirmam (UltimaObs fica na última confirmação).
    """
    rows: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for obs in bronze.observations:
        if obs["Status"] != "ok" or obs["HashGeometria"] is None:
            continue  # falha ou KML que não deu para ler: não confirma nenhuma versão
        same_url = current is not None and current["Url"] == obs["Url"]
        if same_url and current["HashGeometria"] == obs["HashGeometria"]:
            current["UltimaObs"] = obs["ObservadoEm"]
            current["Observacoes"] += 1
            if obs["ModificadoEm"] and not (
                current["ModificadoEm"] and current["ModificadoEm"] <= obs["ModificadoEm"]
            ):
                current["ModificadoEm"] = obs["ModificadoEm"]
            continue

        previous = current if same_url else None
        if previous:
            previous["SubstituidaEm"] = obs["ObservadoEm"]
        parsed = bronze.parsed[obs["Sha256"]]
        current = {
            "Url": obs["Url"],
            "Arquivo": obs["Arquivo"],
            "LinhaNumero": obs["LinhaNumero"],
            "Sentido": obs["Sentido"],
            "Versao": previous["Versao"] + 1 if previous else 1,
            "Sha256": obs["Sha256"],
            "HashGeometria": parsed.hash_geometria,
            "Nome": parsed.nome,
            "Geometria": parsed.geometria,
            "Pontos": parsed.pontos,
            "TrocaApos": previous["UltimaObs"] if previous else None,
            "PrimeiraObs": obs["ObservadoEm"],
            "UltimaObs": obs["ObservadoEm"],
            "SubstituidaEm": None,
            "Observacoes": 1,
            "ModificadoEm": obs["ModificadoEm"],
        }
        rows.append(current)
    set_validity(rows)
    return rows


def set_validity(rows: list[dict[str, Any]]) -> None:
    """ValidoDesde/ValidoAte/Precisao de cada versão (rows em ordem de Url e Versao).

    O Last-Modified só vale se cair na janela da troca (TrocaApos, PrimeiraObs]: fora dela o
    servidor contradiz o que vimos (relógio errado, arquivo copiado com data antiga).
    """
    for row in rows:
        modified, after = row["ModificadoEm"], row["TrocaApos"]
        if modified and (after is None or after < modified) and modified <= row["PrimeiraObs"]:
            row["ValidoDesde"], row["Precisao"] = modified, "exata"
        else:
            row["ValidoDesde"] = row["PrimeiraObs"]
            row["Precisao"] = "desde_1a_obs" if after is None else "janela"
    for i, row in enumerate(rows):
        following = rows[i + 1] if i + 1 < len(rows) else None
        same_url = following is not None and following["Url"] == row["Url"]
        row["ValidoAte"] = following["ValidoDesde"] if same_url else None


def bronze_to_silver(settings: Settings) -> LoadResult:
    bronze = read_bronze(settings)
    versions = build_versions(bronze)
    obs_table = pa.Table.from_pylist(
        bronze.observations, schema=observacao_schema(settings.timezone)
    )
    ver_table = pa.Table.from_pylist(versions, schema=versao_schema(settings.timezone))

    # confere as duas antes de gravar qualquer uma
    out = silver_dir(settings)
    p_obs, p_ver = out / "kml_observacao.parquet", out / "kml_versao.parquet"
    for table, path in ((obs_table, p_obs), (ver_table, p_ver)):
        current = pq.read_metadata(path).num_rows if path.exists() else 0
        if table.num_rows < current:
            raise SilverShrinkError(
                f"{path.name}: o bronze gera {table.num_rows} linhas, mas o silver tem "
                f"{current}. Nada foi alterado; confira o bronze."
            )
    write_parquet(obs_table, p_obs)
    write_parquet(ver_table, p_ver)

    if bronze.rejected:
        write_json(bronze.rejected, quarantine_path(settings))
        logger.warning(
            "%d arquivos de KML rejeitados, veja %s",
            len(bronze.rejected),
            quarantine_path(settings),
        )
    else:
        quarantine_path(settings).unlink(missing_ok=True)

    logger.info(
        "silver %s: %d observações, %d versões em %s",
        DATASET,
        obs_table.num_rows,
        ver_table.num_rows,
        out,
    )
    return LoadResult(
        observacoes=obs_table.num_rows,
        versoes=ver_table.num_rows,
        rejected=len(bronze.rejected),
        path_observacao=p_obs,
        path_versao=p_ver,
    )
