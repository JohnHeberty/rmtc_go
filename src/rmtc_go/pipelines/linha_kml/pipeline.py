from datetime import date, datetime, timedelta

from rmtc_go.core.http import build_session
from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings, get_settings
from rmtc_go.pipelines.locate_bus.config import HEADERS

from .config import DISCOVERY_DAYS, REQUEST_DELAY_SECONDS
from .dedup import DedupResult, remove_duplicates
from .extract import ExtractResult, discover_targets, extract_targets, save_fetch_log
from .load import LoadResult, bronze_to_silver
from .uso import UsoResult, build_pending

logger = get_logger(__name__)


def discovery_days(today: date, n: int = DISCOVERY_DAYS) -> list[date]:
    """Os últimos n dias, incluindo hoje, em ordem."""
    return [today - timedelta(days=i) for i in range(n - 1, -1, -1)]


def extract(settings: Settings | None = None) -> ExtractResult:
    """E: baixa o KML de cada linha/sentido visto nos últimos dias; grava só o que mudou."""
    settings = settings or get_settings()
    now = datetime.now(settings.tz)
    days = discovery_days(now.date())
    targets = discover_targets(days, settings)
    if not targets:
        logger.warning("nenhuma URL de KML no silver de posições de %s a %s", days[0], days[-1])
        return ExtractResult()

    with build_session(settings.http_retries, HEADERS) as session:
        result = extract_targets(session, targets, settings, delay=REQUEST_DELAY_SECONDS)
    save_fetch_log(result.entries, days, now, settings)

    logger.info(
        "bronze linha_kml: %d traçados, %d versões novas, %d sem mudança, %d falhas",
        result.targets,
        len(result.saved),
        result.unchanged,
        len(result.failed),
    )
    if result.failed and len(result.failed) == result.targets:
        raise RuntimeError("nenhum KML foi baixado (RMTC fora do ar?)")
    return result


def dedup(settings: Settings | None = None) -> DedupResult:
    """Apaga do bronze as versões de cada traçado iguais à anterior."""
    return remove_duplicates(settings or get_settings())


def load(settings: Settings | None = None) -> LoadResult:
    """L: reconstrói o silver de traçados a partir do bronze."""
    return bronze_to_silver(settings or get_settings())


def uso(settings: Settings | None = None) -> list[UsoResult]:
    """Silver linha_kml_uso: reconstrói os dias recentes e os que ainda não têm uso."""
    settings = settings or get_settings()
    return build_pending(discovery_days(datetime.now(settings.tz).date()), settings)
