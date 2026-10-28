"""Silver linha_kml_uso: qual KML cada linha/sentido usou, e quando, a partir das posições.

As posições já trazem, a cada coleta, a URL do KML de cada ônibus. Essa tabela resume isso
em períodos, com a precisão da coleta (1 min), por dia. Responde "qual KML a linha X usava
em tal hora" e "quando ela trocou de KML" (ex.: a linha 004 usa EixoT9-004ida às 04h e
Eixo85-004ida no resto do dia).
"""

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import SILVER, list_files, partition_dir, write_parquet

from .config import DATASET, SENTIDOS, SOURCE_DATASET, USO_DATASET, USO_GAP_MINUTES
from .load import SilverShrinkError
from .schemas import uso_schema

logger = get_logger(__name__)


@dataclass(frozen=True)
class UsoResult:
    day: date
    path: Path
    periodos: int
    coletas: int


def uso_path(day: date, settings: Settings) -> Path:
    """silver/linha_kml/uso/year=/month=/day=/linha_kml_uso_YYYYMMDD.parquet: fica junto do
    resto da linha_kml, numa subpasta própria porque é particionado por dia."""
    return (
        partition_dir(settings.data_dir / SILVER, DATASET, "uso", day)
        / f"{USO_DATASET}_{day:%Y%m%d}.parquet"
    )


def positions_files(day: date, settings: Settings) -> list[Path]:
    return list_files(partition_dir(settings.data_dir, SILVER, SOURCE_DATASET, day), "*.parquet")


def periods(df: pd.DataFrame, gap: timedelta) -> pd.DataFrame:
    """(ColetadoEm, LinhaNumero, Sentido, Url) -> períodos por linha/sentido/arquivo."""
    df = df.drop_duplicates().sort_values(["LinhaNumero", "Sentido", "Url", "ColetadoEm"])
    key = ["LinhaNumero", "Sentido", "Url"]
    new_period = df.groupby(key)["ColetadoEm"].diff() > gap
    df = df.assign(periodo=new_period.groupby([df[k] for k in key]).cumsum())
    out = (
        df.groupby([*key, "periodo"], sort=True)["ColetadoEm"]
        .agg(UsoDesde="min", UsoAte="max", Coletas="count")
        .reset_index()
        .drop(columns="periodo")
    )
    out["Arquivo"] = out["Url"].str.rsplit("/", n=1).str[-1]
    return out.sort_values(["LinhaNumero", "Sentido", "UsoDesde", "Url"]).reset_index(drop=True)


def build_uso(day: date, settings: Settings) -> UsoResult | None:
    """Reconstrói o uso de um dia a partir do silver de posições (None se não houver posições).

    Recusa (SilverShrinkError) se o resultado tiver menos coletas que o atual.
    """
    files = positions_files(day, settings)
    if not files:
        return None

    table = ds.dataset(files, format="parquet").to_table(
        columns=["ColetadoEm", "LinhaNumero", *SENTIDOS]
    )
    raw = table.to_pandas()
    parts = []
    for column, sentido in SENTIDOS.items():
        part = raw[["ColetadoEm", "LinhaNumero", column]].dropna()
        part = part.rename(columns={column: "Url"}).assign(Sentido=sentido)
        part["Url"] = part["Url"].str.replace(r"\?.*$", "", regex=True)  # sem o ?rev=
        parts.append(part)
    df = periods(pd.concat(parts, ignore_index=True), timedelta(minutes=USO_GAP_MINUTES))

    result_table = pa.Table.from_pandas(
        df, schema=uso_schema(settings.timezone), preserve_index=False
    )
    path = uso_path(day, settings)
    coletas = int(df["Coletas"].sum())
    if path.exists():
        current = pc.sum(pq.read_table(path, columns=["Coletas"])["Coletas"]).as_py() or 0
        if coletas < current:
            raise SilverShrinkError(
                f"{path.name}: as posições geram {coletas} coletas, mas o silver tem {current}. "
                "Nada foi alterado; confira o silver de posições."
            )
    write_parquet(result_table, path)
    return UsoResult(day=day, path=path, periodos=len(df), coletas=coletas)


def position_days(settings: Settings) -> list[date]:
    root = settings.data_dir / SILVER / SOURCE_DATASET
    days = []
    for directory in root.glob("year=*/month=*/day=*"):
        try:
            days.append(date(*(int(part.split("=")[1]) for part in directory.parts[-3:])))
        except ValueError:
            continue
    return sorted(days)


def days_to_build(recent: list[date], settings: Settings) -> list[date]:
    """Dias recentes (podem ter ganho posições) + dias antigos que ainda não têm uso."""
    recent_set = set(recent)
    return [
        d for d in position_days(settings) if d in recent_set or not uso_path(d, settings).exists()
    ]


def build_pending(recent: list[date], settings: Settings) -> list[UsoResult]:
    results = []
    for day in days_to_build(recent, settings):
        try:
            if (r := build_uso(day, settings)) is not None:
                results.append(r)
        except SilverShrinkError as e:
            logger.error("uso de %s não reconstruído: %s", day, e)
    if results:
        logger.info(
            "silver %s: %s",
            USO_DATASET,
            ", ".join(f"{r.day}: {r.periodos} períodos" for r in results),
        )
    return results
