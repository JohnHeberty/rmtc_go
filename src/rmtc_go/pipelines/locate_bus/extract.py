import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from requests import HTTPError, Session

from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import BRONZE, list_files, partition_dir, read_json, write_json_new

from .config import DATASET, DEFAULT_LINHA, build_params
from .schemas import BronzeMetadata, BronzeRecord


class ExtractError(RuntimeError):
    """A API respondeu, mas com algo que não dá para usar."""


def source_url(settings: Settings) -> str:
    return urljoin(settings.base_url, settings.prefix_url)


def payload_hash(payload: dict[str, Any]) -> str:
    """SHA-256 do payload em JSON canônico (chaves ordenadas): independe de horário e gzip."""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def bronze_dir(day: date, settings: Settings) -> Path:
    return partition_dir(settings.data_dir, BRONZE, DATASET, day)


def bronze_files(day: date, settings: Settings) -> list[Path]:
    """Coletas do dia. Lê também o .json sem compressão das coletas antigas."""
    return list_files(bronze_dir(day, settings), "*.json.gz", "*.json")


def bronze_hash(path: Path) -> str | None:
    """Hash do payload de uma coleta (recalculado se for antiga); None se o arquivo não abrir."""
    try:
        record = read_json(path)
        return record["metadata"].get("payload_sha256") or payload_hash(record["payload"])
    except ValueError, OSError, EOFError, KeyError, TypeError, AttributeError:
        return None


def last_bronze_hash(day: date, settings: Settings) -> str | None:
    files = bronze_files(day, settings)
    return bronze_hash(files[-1]) if files else None


def fetch(
    session: Session, url: str, linha: str = DEFAULT_LINHA, timeout: float = 30
) -> dict[str, Any]:
    """Busca na API. Não valida o conteúdo nem grava nada."""
    response = session.get(url, params=build_params(linha), timeout=timeout)
    try:
        response.raise_for_status()
    except HTTPError as e:
        raise ExtractError(f"RMTC respondeu {response.status_code}: {response.text[:500]}") from e

    try:
        payload = response.json()
    except ValueError as e:
        raise ExtractError(f"Resposta da RMTC não é JSON: {response.text[:500]}") from e

    if not isinstance(payload, dict):
        raise ExtractError(f"Resposta da RMTC com formato inesperado: {type(payload).__name__}")
    return payload


def save_raw(
    payload: dict[str, Any],
    collected_at: datetime,
    linha: str,
    source_url: str,
    settings: Settings,
) -> Path:
    """Grava o payload bruto no bronze, sem alterar nada nele."""
    if collected_at.tzinfo is None:
        raise ValueError("collected_at precisa ter timezone")

    record = BronzeRecord(
        metadata=BronzeMetadata(
            dataset=DATASET,
            source_url=source_url,
            linha=linha,
            collected_at=collected_at,
            payload_sha256=payload_hash(payload),
        ),
        payload=payload,
    )
    path = bronze_dir(collected_at.date(), settings) / f"{collected_at:%Y%m%dT%H%M%S}.json.gz"
    return write_json_new(record.model_dump(mode="json"), path)
