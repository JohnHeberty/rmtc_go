from dataclasses import dataclass
from datetime import date
from pathlib import Path

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import file_stem

from .compact import build_daily
from .extract import bronze_files, bronze_hash
from .load import daily_parquet, quarantine_dir, silver_dir

logger = get_logger(__name__)


@dataclass(frozen=True)
class DedupResult:
    day: date
    files: int  # coletas verificadas
    removed: list[str]  # coletas apagadas (nome sem extensão)


def find_duplicates(files: list[Path]) -> list[Path]:
    """Coletas com o payload igual ao da coleta anterior mantida (em ordem de horário).

    Só compara com a anterior: um mesmo payload que volta depois de outro diferente é mantido.
    Arquivos que não abrem não são tocados (o load os manda para a quarentena).
    """
    duplicates: list[Path] = []
    previous: str | None = None
    for path in files:
        current = bronze_hash(path)
        if current is None:
            continue
        if current == previous:
            duplicates.append(path)
        else:
            previous = current
    return duplicates


def remove_duplicates(day: date, settings: Settings) -> DedupResult:
    """Apaga do bronze, do silver e da quarentena as coletas iguais à anterior no dia."""
    files = bronze_files(day, settings)
    duplicates = find_duplicates(files)

    for path in duplicates:
        stem = file_stem(path)
        (silver_dir(day, settings) / f"{stem}.parquet").unlink(missing_ok=True)
        (quarantine_dir(day, settings) / f"{stem}.json").unlink(missing_ok=True)
        path.unlink()  # bronze por último: se falhar no meio, a próxima execução termina

    removed = [file_stem(p) for p in duplicates]
    if removed:
        logger.info("dedup %s: %d coletas duplicadas removidas: %s", day, len(removed), removed)
        if daily_parquet(day, settings).exists():
            # dia já compactado: as linhas das duplicatas estão no arquivo do dia
            build_daily(day, settings, allow_shrink=True)
    return DedupResult(day=day, files=len(files), removed=removed)
