from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydantic import ValidationError

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import (
    QUARANTINE,
    SILVER,
    file_stem,
    list_files,
    partition_dir,
    read_json,
    write_json,
    write_parquet,
)

from .config import DATASET, STATUS_SUCESSO
from .extract import bronze_files, payload_hash
from .schemas import BronzeRecord, BusRaw, BusSilver, LocateBusResponse, silver_schema

logger = get_logger(__name__)


class NoDataError(RuntimeError):
    """Não há nenhuma coleta no bronze para o dia pedido."""


@dataclass(frozen=True)
class LoadResult:
    day: date
    path: Path
    files: int  # arquivos do bronze processados nesta execução
    skipped: int  # arquivos que já estavam no silver
    rows: int
    rejected: int


def silver_dir(day: date, settings: Settings) -> Path:
    return partition_dir(settings.data_dir, SILVER, DATASET, day)


def quarantine_dir(day: date, settings: Settings) -> Path:
    return partition_dir(settings.data_dir, QUARANTINE, DATASET, day)


def parse_bronze_file(
    path: Path, settings: Settings
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Valida uma coleta do bronze. Retorna (linhas válidas, rejeitados)."""
    try:
        record = BronzeRecord.model_validate(read_json(path))
        response = LocateBusResponse.model_validate(record.payload)
    except (ValueError, OSError, EOFError) as e:  # JSON/gzip corrompido ou envelope inválido
        return [], [{"file": path.name, "error": str(e)}]

    if response.status != STATUS_SUCESSO:
        return [], [{"file": path.name, "error": f"status da API: {response.status}"}]

    rows: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    collected_at = record.metadata.collected_at.astimezone(settings.tz)
    sha256 = record.metadata.payload_sha256 or payload_hash(record.payload)
    for bus in response.onibus:
        try:
            raw = BusRaw.model_validate(bus)
            rows.append(BusSilver.from_raw(raw, collected_at, sha256).model_dump())
        except ValidationError as e:
            rejected.append(
                {"file": path.name, "record": bus, "error": e.errors(include_url=False)}
            )
    return rows, rejected


def load_bronze_file(path: Path, day: date, settings: Settings) -> tuple[int, int]:
    """Carrega uma coleta do bronze no silver (1:1, mesmo nome). Retorna (linhas, rejeitados).

    Sempre grava o parquet, mesmo vazio, para marcar a coleta como carregada.
    """
    rows, rejected = parse_bronze_file(path, settings)
    stem = file_stem(path)

    table = pa.Table.from_pylist(rows, schema=silver_schema(settings.timezone))
    write_parquet(table, silver_dir(day, settings) / f"{stem}.parquet")
    write_quarantine(day, stem, rejected, settings)
    return len(rows), len(rejected)


def write_quarantine(day: date, stem: str, rejected: list[dict[str, Any]], settings: Settings):
    quarantine = quarantine_dir(day, settings) / f"{stem}.json"
    if rejected:
        write_json(rejected, quarantine)
    else:
        quarantine.unlink(missing_ok=True)  # reprocessamento limpo não deixa quarentena antiga


def collection_parquets(day: date, settings: Settings) -> list[Path]:
    """Parquets do silver de uma coleta cada (YYYYMMDDTHHMMSS.parquet), antes da compactação."""
    return list_files(silver_dir(day, settings), "????????T??????.parquet")


def daily_parquet(day: date, settings: Settings) -> Path:
    """Parquet único do dia, gerado pela compactação de um dia fechado."""
    return silver_dir(day, settings) / f"{DATASET}_{day:%Y%m%d}.parquet"


def bronze_to_silver(day: date, settings: Settings, full: bool = False) -> LoadResult:
    """Carrega no silver as coletas do dia que ainda não foram carregadas.

    Tipado e validado, 1:1 com o bruto, sem regra de negócio. Cada arquivo do bronze vira um
    parquet com o mesmo nome, então reprocessar é idempotente. `full=True` recarrega o dia todo.
    """
    files = bronze_files(day, settings)
    if not files:
        raise NoDataError(f"Nenhuma coleta no bronze de {day}")

    out = silver_dir(day, settings)
    pending = [p for p in files if full or not (out / f"{file_stem(p)}.parquet").exists()]

    rows = rejected = 0
    for path in pending:
        n_rows, n_rejected = load_bronze_file(path, day, settings)
        rows += n_rows
        rejected += n_rejected

    if rejected:
        logger.warning(
            "%d registros rejeitados em %s, veja %s", rejected, day, quarantine_dir(day, settings)
        )
    logger.info(
        "silver %s: %d arquivos novos (%d já carregados) -> %d linhas em %s",
        day,
        len(pending),
        len(files) - len(pending),
        rows,
        out,
    )
    return LoadResult(
        day=day,
        path=out,
        files=len(pending),
        skipped=len(files) - len(pending),
        rows=rows,
        rejected=rejected,
    )
