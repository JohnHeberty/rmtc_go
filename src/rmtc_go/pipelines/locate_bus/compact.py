"""Compactação do silver: os parquets de uma coleta cada de um dia fechado viram um só.

O arquivo do dia é sempre reconstruído do bronze (a fonte da verdade), nunca lendo e
acrescentando ao silver atual, então um erro numa versão não se propaga para a próxima.
"""

from datetime import date, datetime

import pyarrow as pa

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import SILVER, file_stem, parquet_rows, write_parquet

from .config import DATASET
from .extract import bronze_files
from .load import (
    LoadResult,
    NoDataError,
    collection_parquets,
    daily_parquet,
    parse_bronze_file,
    quarantine_dir,
    write_quarantine,
)
from .schemas import silver_schema

logger = get_logger(__name__)


class SilverShrinkError(RuntimeError):
    """O silver reconstruído teria menos linhas que o atual: o bronze perdeu arquivos?"""


def is_closed(day: date, settings: Settings) -> bool:
    """Dia que já terminou no fuso do projeto (não recebe mais coletas)."""
    return day < datetime.now(settings.tz).date()


def build_daily(day: date, settings: Settings, allow_shrink: bool = False) -> LoadResult:
    """Reconstrói o silver do dia num parquet único a partir do bronze.

    Ordem segura: grava o arquivo do dia (atômico e conferido) e só depois apaga os parquets
    por coleta. Se cair no meio, rodar de novo termina o serviço sem duplicar nem perder linhas.

    Recusa (SilverShrinkError) se o resultado tiver menos linhas que o silver atual, a não ser
    com allow_shrink=True (usado pelo dedup, que remove coletas de propósito).
    """
    files = bronze_files(day, settings)
    if not files:
        raise NoDataError(f"Nenhuma coleta no bronze de {day}")

    target = daily_parquet(day, settings)
    small = collection_parquets(day, settings)
    # Se sobrou o arquivo do dia e os por coleta (queda no meio da compactação), os por coleta
    # repetem o conteúdo do dia: a referência é o maior dos dois, não a soma.
    current_rows = max(parquet_rows(small), parquet_rows([target]) if target.exists() else 0)

    rows, n_rejected = [], 0
    for path in files:
        file_rows, rejected = parse_bronze_file(path, settings)
        rows.extend(file_rows)
        n_rejected += len(rejected)
        write_quarantine(day, file_stem(path), rejected, settings)

    table = pa.Table.from_pylist(rows, schema=silver_schema(settings.timezone))
    if table.num_rows < current_rows and not allow_shrink:
        raise SilverShrinkError(
            f"silver {day}: o bronze gera {table.num_rows} linhas, mas o silver tem "
            f"{current_rows}. Nada foi alterado; confira o bronze antes de reprocessar."
        )

    write_parquet(table, target)
    for path in small:
        path.unlink()

    if n_rejected:
        logger.warning(
            "%d registros rejeitados em %s, veja %s", n_rejected, day, quarantine_dir(day, settings)
        )
    logger.info(
        "silver %s compactado: %d coletas -> %d linhas em %s (%d arquivos por coleta removidos)",
        day,
        len(files),
        table.num_rows,
        target,
        len(small),
    )
    return LoadResult(
        day=day, path=target, files=len(files), skipped=0, rows=table.num_rows, rejected=n_rejected
    )


def days_to_compact(settings: Settings) -> list[date]:
    """Dias fechados que ainda têm parquets por coleta no silver."""
    root = settings.data_dir / SILVER / DATASET
    days = []
    for directory in root.glob("year=*/month=*/day=*"):
        try:
            day = date(*(int(part.split("=")[1]) for part in directory.parts[-3:]))
        except ValueError:
            continue
        if is_closed(day, settings) and collection_parquets(day, settings):
            days.append(day)
    return sorted(days)


def compact_closed_days(settings: Settings) -> list[LoadResult]:
    """Compacta todos os dias fechados pendentes. Um dia com problema não impede os outros."""
    results = []
    for day in days_to_compact(settings):
        try:
            results.append(build_daily(day, settings))
        except (NoDataError, SilverShrinkError) as e:
            # não falha o flow (roda a cada minuto), mas fica visível no log do Prefect
            logger.error("compactação de %s não feita: %s", day, e)
    return results
