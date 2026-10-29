"""Silver linha_kml_uso: qual KML cada linha/sentido usou, e quando, a partir das posições.

As posições já trazem, a cada coleta, a URL do KML de cada ônibus. Essa tabela resume isso
em períodos, com a precisão da coleta (1 min), por dia. Responde "qual KML a linha X usava
em tal hora" e "quando ela trocou de KML" (ex.: a linha 004 usa EixoT9-004ida às 04h e
Eixo85-004ida no resto do dia).

O KML é da linha, não do ônibus: a RMTC manda o de ida e o de volta em toda posição, e troca
para todos os ônibus da linha juntos (checado em check_assumptions). Períodos com o KML null
ficam como um período sem Url, para a linha não sumir (ex.: falha da RMTC das 04h).
"""

import json
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
from rmtc_go.pipelines.locate_bus.compact import is_closed

from .config import DATASET, SENTIDOS, SOURCE_DATASET, USO_DATASET, USO_GAP_MINUTES
from .load import SilverShrinkError
from .schemas import uso_schema

logger = get_logger(__name__)

# metadado do parquet de uso: de quais parquets de posições (e com quantas linhas) ele veio
FONTE_KEY = b"fonte"


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


def source_fingerprint(files: list[Path]) -> str:
    """Nome e nº de linhas de cada parquet de posições (só o rodapé): muda se o dia mudar."""
    return json.dumps([[p.name, pq.read_metadata(p).num_rows] for p in files])


def stored_fingerprint(path: Path) -> str | None:
    if not path.exists():
        return None
    value = (pq.read_schema(path).metadata or {}).get(FONTE_KEY)
    return value.decode() if value else None


def read_positions(day: date, settings: Settings) -> tuple[list[Path], pa.Table]:
    """Posições do dia. Se um arquivo sumir no meio (a compactação do dia rodando ao mesmo
    tempo), lista de novo e tenta mais uma vez."""
    for attempt in range(2):
        files = positions_files(day, settings)
        try:
            table = ds.dataset(files, format="parquet").to_table(
                columns=["ColetadoEm", "LinhaNumero", *SENTIDOS]
            )
            return files, table
        except OSError:
            if attempt:
                raise
            logger.warning("posições de %s mudaram durante a leitura, lendo de novo", day)
    raise AssertionError("inalcançável")


def check_assumptions(df: pd.DataFrame) -> list[str]:
    """Premissas da tabela que, se quebrarem, mudam como ela deve ser lida. Só avisa."""
    df = df.dropna(subset=["Url"])
    problems = []
    per_collection = df.groupby(["ColetadoEm", "LinhaNumero", "Sentido"])["Url"].nunique()
    if (conflicts := per_collection[per_collection > 1]).size:
        linhas = sorted({f"{linha}/{sentido}" for _, linha, sentido in conflicts.index})
        problems.append(
            f"{conflicts.size} coletas com mais de um KML para a mesma linha/sentido "
            f"(o KML deixou de ser da linha?): {linhas[:10]}"
        )
    pairs = df.drop_duplicates(["Url", "LinhaNumero", "Sentido"])
    if (shared := pairs[pairs.duplicated("Url", keep=False)]).size:
        problems.append(
            "KML usado por mais de uma linha/sentido (ou ida = volta): "
            f"{sorted(shared['Url'].unique())[:10]}"
        )
    urls = df.drop_duplicates("Url")
    urls = urls.assign(Arquivo=urls["Url"].str.rsplit("/", n=1).str[-1])
    if (same_name := urls[urls.duplicated("Arquivo", keep=False)]).size:
        problems.append(
            "URLs diferentes com o mesmo nome de arquivo (colidem no bronze): "
            f"{sorted(same_name['Url'])[:10]}"
        )
    for problem in problems:
        logger.warning("linha_kml_uso: %s", problem)
    return problems


def periods(df: pd.DataFrame, gap: timedelta) -> pd.DataFrame:
    """(ColetadoEm, LinhaNumero, Sentido, Url) -> períodos por linha/sentido/arquivo.

    Url null (KML não informado) é um grupo como os outros.
    """
    df = df.drop_duplicates().sort_values(["LinhaNumero", "Sentido", "Url", "ColetadoEm"])
    key = ["LinhaNumero", "Sentido", "Url"]
    new_period = df.groupby(key, dropna=False)["ColetadoEm"].diff() > gap
    df = df.assign(periodo=new_period.groupby([df[k] for k in key], dropna=False).cumsum())
    out = (
        df.groupby([*key, "periodo"], sort=True, dropna=False)["ColetadoEm"]
        .agg(UsoDesde="min", UsoAte="max", Coletas="count")
        .reset_index()
        .drop(columns="periodo")
    )
    out["Arquivo"] = out["Url"].str.rsplit("/", n=1).str[-1]
    out = out.astype({"Url": object, "Arquivo": object})
    out.loc[out["Url"].isna(), ["Url", "Arquivo"]] = None
    return out.sort_values(["LinhaNumero", "Sentido", "UsoDesde", "Url"]).reset_index(drop=True)


def build_uso(day: date, settings: Settings, allow_shrink: bool = False) -> UsoResult | None:
    """Reconstrói o uso de um dia a partir do silver de posições (None se não houver posições).

    Num dia fechado, recusa (SilverShrinkError) se o resultado tiver menos coletas que o atual,
    a não ser com allow_shrink (posições deduplicadas de propósito). O dia corrente sempre é
    refeito: o dedup das posições pode tirar coletas dele.
    """
    if not positions_files(day, settings):
        return None
    files, table = read_positions(day, settings)
    raw = table.to_pandas()
    parts = []
    for column, sentido in SENTIDOS.items():
        part = raw[["ColetadoEm", "LinhaNumero", column]]
        part = part.rename(columns={column: "Url"}).assign(Sentido=sentido)
        # string mesmo quando o dia inteiro veio sem KML (senão o pandas lê como float)
        part["Url"] = (
            part["Url"].astype("string").str.replace(r"\?.*$", "", regex=True)
        )  # sem o ?rev=
        parts.append(part)
    positions = pd.concat(parts, ignore_index=True)
    check_assumptions(positions)
    df = periods(positions, timedelta(minutes=USO_GAP_MINUTES))

    result_table = pa.Table.from_pandas(
        df, schema=uso_schema(settings.timezone), preserve_index=False
    ).replace_schema_metadata({FONTE_KEY: source_fingerprint(files)})
    path = uso_path(day, settings)
    coletas = int(df["Coletas"].sum())
    if path.exists() and is_closed(day, settings) and not allow_shrink:
        current = pc.sum(pq.read_table(path, columns=["Coletas"])["Coletas"]).as_py() or 0
        if coletas < current:
            raise SilverShrinkError(
                f"{path.name}: as posições geram {coletas} coletas, mas o silver tem {current}. "
                "Nada foi alterado; confira o silver de posições (ou use --allow-shrink)."
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


def days_to_build(settings: Settings) -> list[date]:
    """Dias sem uso ou cujas posições mudaram desde a última vez (ex.: dia reprocessado)."""
    return [
        d
        for d in position_days(settings)
        if stored_fingerprint(uso_path(d, settings))
        != source_fingerprint(positions_files(d, settings))
    ]


def build_pending(settings: Settings) -> list[UsoResult]:
    """Reconstrói os dias pendentes. Um dia recusado não impede os outros, mas no fim levanta
    SilverShrinkError: o flow fica vermelho em vez de só registrar no log."""
    results, errors = [], []
    for day in days_to_build(settings):
        try:
            if (r := build_uso(day, settings)) is not None:
                results.append(r)
        except SilverShrinkError as e:
            logger.error("uso de %s não reconstruído: %s", day, e)
            errors.append(f"{day}: {e}")
    if results:
        logger.info(
            "silver %s: %s",
            USO_DATASET,
            ", ".join(f"{r.day}: {r.periodos} períodos" for r in results),
        )
    if errors:
        raise SilverShrinkError("; ".join(errors))
    return results
