"""Utilitários do Prefect compartilhados entre as pipelines."""

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from prefect.states import Crashed

from .logging import get_logger

logger = get_logger(__name__)

ORPHAN_STATES = [StateType.RUNNING, StateType.PENDING, StateType.CANCELLING]


def crash_orphaned_runs(deployment_name: str) -> int:
    """Marca como Crashed as execuções do deployment que ficaram presas em execução.

    Chamado ao subir o `serve`: se o processo está começando, nenhuma execução dele pode estar
    rodando de verdade. Sem isso, uma execução morta num restart do container segura a vaga do
    global_limit=1 e todas as seguintes ficam presas em SCHEDULED (visto em 28/10, 10:04-11:33).

    Seguro porque cada deployment tem um único processo `serve` (um container).
    """
    with get_client(sync_client=True) as client:
        runs = client.read_flow_runs(
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[deployment_name])),
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=ORPHAN_STATES))
            ),
            limit=200,
        )
        for run in runs:
            client.set_flow_run_state(
                run.id, Crashed(message="processo do serve reiniciou durante a execução")
            )
            logger.warning("execução órfã %s (%s) marcada como Crashed", run.name, run.state_name)
    return len(runs)
