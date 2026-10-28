from datetime import date

from prefect import flow, task
from prefect.schedules import Cron

from rmtc_go.core.orchestration import crash_orphaned_runs
from rmtc_go.core.settings import get_settings

from . import pipeline
from .config import DEFAULT_LINHA


# Sem retries na task: a Session já repete 429/5xx com backoff (core/http.py), e a próxima
# execução agendada coleta de novo. Retry nos dois níveis multiplicava as requisições.
@task
def extract_task(linha: str) -> str | None:
    path = pipeline.extract(linha)
    return str(path) if path else None  # None: coleta igual à anterior, não gravada


@task
def load_task(day: date | None, full: bool) -> dict:
    result = pipeline.load(day, full)
    return {
        "day": str(result.day),
        "files": result.files,
        "skipped": result.skipped,
        "rows": result.rows,
        "rejected": result.rejected,
    }


@task
def dedup_task(day: date | None) -> dict:
    result = pipeline.dedup(day)
    return {"day": str(result.day), "files": result.files, "removed": result.removed}


@task
def compact_task() -> list[dict]:
    return [{"day": str(r.day), "files": r.files, "rows": r.rows} for r in pipeline.compact()]


@flow(name="locate-bus")
def locate_bus_flow(
    day: date | None = None,
    linha: str = DEFAULT_LINHA,
    extract: bool = True,
    load: bool = True,
    full: bool = False,
    dedup: bool = True,
    compact: bool = True,
) -> None:
    """Extract (bronze) -> load (silver) -> dedup -> compactação dos dias fechados.

    Backfill: extract=False, day=<data>, full=True.
    """
    if extract:
        extract_task(linha)
    if load:
        load_task(day, full)
    if dedup:
        dedup_task(day)
    if compact:
        compact_task()  # sem nada pendente só lista as pastas; na virada do dia compacta o anterior


def serve(cron: str | None = None) -> None:
    """Agenda o flow no cron (padrão: LOCATE_BUS_CRON do .env), no fuso de TIMEZONE."""
    settings = get_settings()
    crash_orphaned_runs("locate-bus-schedule")  # libera a vaga se um restart matou uma execução
    locate_bus_flow.serve(
        name="locate-bus-schedule",
        schedules=[Cron(cron or settings.locate_bus_cron, timezone=settings.timezone)],
        global_limit=1,  # uma execução por vez: não sobrepõe se uma coleta atrasar
    )
