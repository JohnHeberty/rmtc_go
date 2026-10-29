from datetime import date, datetime
from pathlib import Path

from rmtc_go.core.http import build_session
from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings, get_settings

from .coleta import count_sem_kml, save_attempt
from .compact import build_daily, compact_closed_days, is_closed
from .config import DEFAULT_LINHA, HEADERS, STATUS_SUCESSO
from .dedup import DedupResult, remove_duplicates
from .extract import fetch, last_bronze_hash, payload_hash, save_raw, source_url
from .load import LoadResult, bronze_to_silver

logger = get_logger(__name__)


def extract(linha: str = DEFAULT_LINHA, settings: Settings | None = None) -> Path | None:
    """E: busca na API e salva o bruto no bronze. O horário é calculado a cada chamada.

    Não grava se o payload for igual ao da última coleta do dia (retorna None).
    Toda tentativa, com ou sem sucesso, fica no registro de cobertura (coleta.py).
    """
    settings = settings or get_settings()
    url = source_url(settings)
    collected_at = datetime.now(settings.tz)
    attempt: dict = {"status": "erro"}
    try:
        with build_session(settings.http_retries, HEADERS) as session:
            payload = fetch(session, url, linha, settings.http_timeout)

        n_onibus = len(payload.get("onibus") or [])
        attempt = {"status": "vazia", "onibus": n_onibus, "sem_kml": count_sem_kml(payload)}
        if payload.get("status") == STATUS_SUCESSO and n_onibus == 0:
            # Visto em 27/10: a API responde sucesso com a lista vazia por alguns minutos
            logger.warning("API respondeu sucesso sem nenhum ônibus (instabilidade da RMTC?)")

        if payload_hash(payload) == last_bronze_hash(collected_at.date(), settings):
            attempt["status"] = "igual" if n_onibus else "vazia"
            logger.info("bronze: coleta igual à anterior, não gravada (%d ônibus)", n_onibus)
            return None

        path = save_raw(payload, collected_at, linha, url, settings)
        attempt["status"] = "gravada" if n_onibus else "vazia"
        logger.info("bronze: %d ônibus gravados em %s", n_onibus, path)
        return path
    except Exception as e:
        attempt["erro"] = f"{type(e).__name__}: {e}"[:500]
        raise
    finally:
        try:
            save_attempt(collected_at, settings=settings, **attempt)
        except Exception:  # o registro nunca derruba a coleta
            logger.exception("tentativa de coleta não registrada")


def load(
    day: date | None = None, full: bool = False, settings: Settings | None = None
) -> LoadResult:
    """L: carrega o bronze do dia no silver (parquet).

    Dia corrente: incremental, um parquet por coleta (ou todas, se full).
    Dia fechado: reconstrói o parquet único do dia a partir do bronze.
    """
    settings = settings or get_settings()
    day = day or datetime.now(settings.tz).date()
    if is_closed(day, settings):
        return build_daily(day, settings)
    return bronze_to_silver(day, settings, full=full)


def compact(settings: Settings | None = None) -> list[LoadResult]:
    """Compacta num parquet por dia os dias fechados que ainda têm parquets por coleta."""
    return compact_closed_days(settings or get_settings())


def dedup(day: date | None = None, settings: Settings | None = None) -> DedupResult:
    """Apaga as coletas do dia iguais à anterior (bronze, silver e quarentena)."""
    settings = settings or get_settings()
    return remove_duplicates(day or datetime.now(settings.tz).date(), settings)
