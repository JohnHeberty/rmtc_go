"""Silver linha_kml_tracado: o uso de cada linha já resolvido para a versão do KML.

Junta `uso` (qual arquivo a linha usava) com `kml_versao` (como era o arquivo) num lugar só,
para que toda consulta trate a incerteza do mesmo jeito. Cada período do uso é cortado nas
fronteiras de versão e ganha um Status:

- certo: a versão estava confirmada nesse horário
- janela: a troca para esta versão aconteceu em algum momento aqui (TrocaApos, PrimeiraObs]
  e o servidor não deu um Last-Modified coerente; pode ser esta versão ou a anterior
- anterior: antes da 1ª versão conhecida do arquivo (Versao 1 é só o palpite)
- nao_confirmado: depois da última confirmação; provavelmente a versão mais recente,
  confirmada (ou não) no próximo download
- sem_download: arquivo que nunca foi baixado (ainda não, ou nome barrado pelo extract)
- sem_kml: a RMTC não informou o KML (ex.: falha das 04h)

Fora dos períodos, tracado_em responde sem_uso (havia coleta, a linha não apareceu) ou
sem_coleta (não estávamos coletando), a partir da cobertura da locate_bus.
"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import SILVER, partition_dir, write_parquet
from rmtc_go.pipelines.locate_bus.coleta import coleta_path, covered

from .config import DATASET
from .load import silver_dir

logger = get_logger(__name__)

TRACADO_DATASET = "linha_kml_tracado"


@dataclass(frozen=True)
class Segment:
    start: datetime | None  # None: sem limite
    end: datetime | None
    versao: int
    status: str


@dataclass(frozen=True)
class Resposta:
    status: str
    arquivo: str | None = None
    url: str | None = None
    versao: int | None = None
    desde: datetime | None = None
    ate: datetime | None = None


def tracado_schema(timezone: str) -> pa.Schema:
    ts = pa.timestamp("us", tz=timezone)
    return pa.schema(
        [
            ("LinhaNumero", pa.string()),
            ("Sentido", pa.string()),
            ("Url", pa.string()),
            ("Arquivo", pa.string()),
            ("Desde", ts),
            ("Ate", ts),
            ("Versao", pa.int64()),  # junta com kml_versao por (Url, Versao)
            ("Status", pa.string()),
            # arquivo do período anterior da linha/sentido (entre dias, ignorando sem_kml):
            # diferente de Arquivo = a linha trocou de KML aqui
            ("ArquivoAnterior", pa.string()),
        ]
    )


def tracado_path(day: date, settings: Settings) -> Path:
    return (
        partition_dir(settings.data_dir / SILVER, DATASET, "tracado", day)
        / f"{TRACADO_DATASET}_{day:%Y%m%d}.parquet"
    )


def uso_files(settings: Settings) -> list[Path]:
    return sorted(
        (settings.data_dir / SILVER / DATASET / "uso").glob("year=*/month=*/day=*/*.parquet")
    )


def segments(versions: list[dict[str, Any]]) -> list[Segment]:
    """Linha do tempo de um arquivo: o que se sabe do conteúdo em cada intervalo [start, end)."""
    first = versions[0]
    out = [Segment(None, first["ValidoDesde"], first["Versao"], "anterior")]
    for cur, nxt in zip(versions, [*versions[1:], None], strict=True):
        if nxt is None:
            out.append(Segment(cur["ValidoDesde"], cur["UltimaObs"], cur["Versao"], "certo"))
            out.append(Segment(cur["UltimaObs"], None, cur["Versao"], "nao_confirmado"))
        elif nxt["Precisao"] == "exata":
            out.append(Segment(cur["ValidoDesde"], nxt["ValidoDesde"], cur["Versao"], "certo"))
        else:
            out.append(Segment(cur["ValidoDesde"], nxt["TrocaApos"], cur["Versao"], "certo"))
            out.append(Segment(nxt["TrocaApos"], nxt["ValidoDesde"], nxt["Versao"], "janela"))
    return [s for s in out if s.start is None or s.end is None or s.start < s.end]


def split_period(period: dict[str, Any], timeline: list[Segment] | None) -> list[dict[str, Any]]:
    """Corta um período do uso nas fronteiras da linha do tempo do arquivo."""
    base = {k: period[k] for k in ("LinhaNumero", "Sentido", "Url", "Arquivo")}
    desde, ate = period["UsoDesde"], period["UsoAte"]
    if period["Url"] is None:
        return [base | {"Desde": desde, "Ate": ate, "Versao": None, "Status": "sem_kml"}]
    if not timeline:
        return [base | {"Desde": desde, "Ate": ate, "Versao": None, "Status": "sem_download"}]
    pieces = []
    for s in timeline:
        if (s.end is not None and s.end <= desde) or (s.start is not None and s.start > ate):
            continue
        start = desde if s.start is None else max(desde, s.start)
        end = ate if s.end is None else min(ate, s.end)
        pieces.append(base | {"Desde": start, "Ate": end, "Versao": s.versao, "Status": s.status})
    return pieces


def read_versions(settings: Settings) -> dict[str, list[dict[str, Any]]]:
    path = silver_dir(settings) / "kml_versao.parquet"
    if not path.exists():
        return {}
    columns = ["Url", "Versao", "Precisao", "ValidoDesde", "TrocaApos", "UltimaObs"]
    by_url: dict[str, list[dict[str, Any]]] = {}
    for row in sorted(pq.read_table(path, columns=columns).to_pylist(), key=lambda r: r["Versao"]):
        by_url.setdefault(row["Url"], []).append(row)
    return by_url


def build_tracado(settings: Settings) -> list[Path]:
    """Reconstrói o tracado de todos os dias que têm uso (barato, e é o que faz um
    nao_confirmado de ontem virar certo depois do download de hoje)."""
    timelines = {url: segments(v) for url, v in read_versions(settings).items()}
    files = uso_files(settings)
    if not files:
        return []
    uso = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    uso = uso.astype({"Url": object, "Arquivo": object}).replace({float("nan"): None})
    uso = uso.sort_values(["LinhaNumero", "Sentido", "UsoDesde"], kind="stable")

    # arquivo anterior de cada período, ignorando os sem KML
    known = uso["Arquivo"].where(uso["Url"].notna())
    uso["ArquivoAnterior"] = known.groupby([uso["LinhaNumero"], uso["Sentido"]]).transform(
        lambda s: s.shift().ffill()
    )
    uso = uso.astype({"ArquivoAnterior": object}).replace({float("nan"): None})

    rows = []
    for period in uso.to_dict("records"):
        for piece in split_period(period, timelines.get(period["Url"])):
            rows.append(piece | {"ArquivoAnterior": period["ArquivoAnterior"]})
    if missing := {p["Url"] for p in rows if p["Status"] == "sem_download"}:
        logger.warning(
            "%d KML usados sem nenhuma versão baixada: %s", len(missing), sorted(missing)[:10]
        )

    df = pd.DataFrame(rows)
    df["dia"] = df["Desde"].dt.tz_convert(settings.tz).dt.date
    paths = []
    for day, group in df.groupby("dia"):
        table = pa.Table.from_pandas(
            group.drop(columns="dia"),
            schema=tracado_schema(settings.timezone),
            preserve_index=False,
        )
        paths.append(write_parquet(table, tracado_path(day, settings)))
    logger.info("silver %s: %d períodos em %d dias", TRACADO_DATASET, len(df), len(paths))
    return paths


def tracado_em(linha: str, sentido: str, t: datetime, settings: Settings) -> Resposta:
    """Qual traçado a linha/sentido usava no instante t, e com que certeza."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=settings.tz)
    day = t.astimezone(settings.tz).date()
    path = tracado_path(day, settings)
    if path.exists():
        df = pd.read_parquet(path)
        hit = df[
            (df["LinhaNumero"] == linha)
            & (df["Sentido"] == sentido)
            & (df["Desde"] <= t)
            & (df["Ate"] >= t)
        ].sort_values("Url", na_position="last")
        if len(hit):
            r = hit.iloc[0]
            return Resposta(
                status=r["Status"],
                arquivo=None if pd.isna(r["Arquivo"]) else r["Arquivo"],
                url=None if pd.isna(r["Url"]) else r["Url"],
                versao=None if pd.isna(r["Versao"]) else int(r["Versao"]),
                desde=r["Desde"].to_pydatetime(),
                ate=r["Ate"].to_pydatetime(),
            )
    cobertura = coleta_path(day, settings)
    if cobertura.exists() and covered(pq.read_table(cobertura), t):
        return Resposta(status="sem_uso")
    return Resposta(status="sem_coleta")
