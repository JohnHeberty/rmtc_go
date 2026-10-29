from prefect import flow, task
from prefect.schedules import Cron

from rmtc_go.core.orchestration import crash_orphaned_runs
from rmtc_go.core.settings import get_settings

from . import pipeline


@task
def extract_task() -> dict:
    result = pipeline.extract()
    return {
        "targets": result.targets,
        "saved": len(result.saved),
        "unchanged": result.unchanged,
        "failed": result.failed,
    }


@task
def dedup_task() -> dict:
    result = pipeline.dedup()
    return {"files": result.files, "removed": result.removed}


@task
def load_task() -> dict:
    result = pipeline.load()
    return {
        "observacoes": result.observacoes,
        "versoes": result.versoes,
        "rejected": result.rejected,
    }


@task
def uso_task() -> list[dict]:
    return [
        {"day": str(r.day), "periodos": r.periodos, "coletas": r.coletas} for r in pipeline.uso()
    ]


@task
def tracado_task() -> int:
    return len(pipeline.tracado())


@flow(name="linha-kml")
def linha_kml_flow(
    extract: bool = True,
    dedup: bool = True,
    load: bool = True,
    uso: bool = True,
    tracado: bool = True,
) -> None:
    """Extract (bronze) -> dedup (bronze) -> load (kml_observacao, kml_versao) -> uso -> tracado.

    O dedup vem antes do load porque o silver é reconstruído inteiro a partir do bronze.
    O uso (linha_kml_uso) vem das posições e não depende dos downloads. O tracado junta os
    dois e a cobertura da coleta.
    """
    if extract:
        extract_task()
    if dedup:
        dedup_task()
    if load:
        load_task()
    if uso:
        uso_task()
    if tracado:
        tracado_task()


def serve(cron: str | None = None) -> None:
    """Agenda o flow no cron (padrão: LINHA_KML_CRON do .env), no fuso de TIMEZONE.

    Além do ciclo completo, refaz uso e tracado de hora em hora (sem baixar KML), para que o
    dia corrente não fique parado no horário do download.
    """
    settings = get_settings()
    crash_orphaned_runs("linha-kml-schedule")  # libera a vaga se um restart matou uma execução
    linha_kml_flow.serve(
        name="linha-kml-schedule",
        schedules=[
            Cron(cron or settings.linha_kml_cron, timezone=settings.timezone),
            Cron(
                settings.linha_kml_uso_cron,
                timezone=settings.timezone,
                parameters={"extract": False, "dedup": False, "load": False},
            ),
        ],
        global_limit=1,
    )
