"""Cobertura da coleta: um registro por tentativa, com ou sem posições gravadas.

O silver de posições só tem as coletas gravadas: uma resposta igual à anterior, vazia ou com
falha não deixa linha nele. Sem este registro não dá para separar "a linha estava sem ônibus"
de "não estávamos coletando" (ex.: 27/10 18:42, 4 h sem coleta).

bronze/locate_bus_coleta/year=/month=/day=/<ts>.json.gz   # uma tentativa
silver/locate_bus_coleta/year=/month=/day=/locate_bus_coleta_YYYYMMDD.parquet
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import (
    BRONZE,
    SILVER,
    list_files,
    partition_dir,
    read_json,
    write_json_new,
    write_parquet,
)

from .config import DATASET

logger = get_logger(__name__)

COLETA_DATASET = "locate_bus_coleta"

# metadado do parquet: nº de tentativas e de linhas de posições de que ele veio
FONTE_KEY = b"fonte"

# "gravada": posições novas no silver; "igual": igual à anterior (não gravada, mas coberta);
# "vazia": a API respondeu sem ônibus; "erro": falhou; "inferida": dia anterior a este
# registro, reconstruído das posições (só as coletas gravadas)
COBERTA = ("gravada", "igual", "inferida")

# Tentativa coberta vale para os minutos vizinhos (a coleta é 1x por minuto)
COBERTURA_MINUTOS = 2


def coleta_schema(timezone: str) -> pa.Schema:
    return pa.schema(
        [
            ("TentativaEm", pa.timestamp("us", tz=timezone)),
            ("Status", pa.string()),
            ("Onibus", pa.int64()),
            ("OnibusSemKml", pa.int64()),  # mede a falha da RMTC das 04h (KML null)
            ("Erro", pa.string()),
        ]
    )


def attempts_dir(day: date, settings: Settings) -> Path:
    return partition_dir(settings.data_dir, BRONZE, COLETA_DATASET, day)


def coleta_path(day: date, settings: Settings) -> Path:
    return (
        partition_dir(settings.data_dir, SILVER, COLETA_DATASET, day)
        / f"{COLETA_DATASET}_{day:%Y%m%d}.parquet"
    )


def count_sem_kml(payload: dict[str, Any]) -> int:
    """Ônibus sem o KML de ida (vêm os dois null juntos, visto em 28/10)."""
    return sum(
        1
        for bus in payload.get("onibus") or []
        if isinstance(bus, dict) and not (bus.get("Linha") or {}).get("LinhaKMLIda")
    )


def save_attempt(
    attempted_at: datetime,
    status: str,
    settings: Settings,
    onibus: int = 0,
    sem_kml: int = 0,
    erro: str | None = None,
) -> Path:
    record = {
        "tentativa_em": attempted_at.isoformat(),
        "status": status,
        "onibus": onibus,
        "sem_kml": sem_kml,
        "erro": erro,
    }
    path = attempts_dir(attempted_at.date(), settings) / f"{attempted_at:%Y%m%dT%H%M%S}.json.gz"
    return write_json_new(record, path)


def rows_from_attempts(files: list[Path], settings: Settings) -> list[dict[str, Any]]:
    rows = []
    for path in files:
        try:
            r = read_json(path)
            rows.append(
                {
                    "TentativaEm": datetime.fromisoformat(r["tentativa_em"]).astimezone(
                        settings.tz
                    ),
                    "Status": r["status"],
                    "Onibus": r["onibus"],
                    "OnibusSemKml": r["sem_kml"],
                    "Erro": r["erro"],
                }
            )
        except (ValueError, OSError, EOFError, KeyError, TypeError) as e:
            logger.warning("tentativa de coleta ilegível %s: %s", path.name, e)
    return rows


def positions_files(day: date, settings: Settings) -> list[Path]:
    return list_files(partition_dir(settings.data_dir, SILVER, DATASET, day), "*.parquet")


def rows_from_positions(day: date, settings: Settings) -> list[dict[str, Any]]:
    """Antes do registro existir: as coletas gravadas no silver de posições."""
    files = positions_files(day, settings)
    if not files:
        return []
    table = ds.dataset(files, format="parquet").to_table(columns=["ColetadoEm", "LinhaKMLIda"])
    counts = table.group_by("ColetadoEm").aggregate(
        [("ColetadoEm", "count"), ("LinhaKMLIda", "count")]
    )
    return [
        {
            "TentativaEm": at,
            "Status": "inferida",
            "Onibus": n,
            "OnibusSemKml": n - with_kml,
            "Erro": None,
        }
        for at, n, with_kml in zip(
            counts["ColetadoEm"].to_pylist(),
            counts["ColetadoEm_count"].to_pylist(),
            counts["LinhaKMLIda_count"].to_pylist(),
            strict=True,
        )
    ]


def source_fingerprint(day: date, settings: Settings) -> str:
    attempts = len(list_files(attempts_dir(day, settings), "*.json.gz"))
    positions = sum(pq.read_metadata(p).num_rows for p in positions_files(day, settings))
    return json.dumps([attempts, positions])


def stored_fingerprint(path: Path) -> str | None:
    if not path.exists():
        return None
    value = (pq.read_schema(path).metadata or {}).get(FONTE_KEY)
    return value.decode() if value else None


def build_cobertura(day: date, settings: Settings) -> Path | None:
    """Reconstrói a cobertura do dia (None se não houver tentativas nem posições).

    Antes da 1ª tentativa registrada (dias antigos, ou o dia em que o registro entrou), usa
    as coletas das posições, marcadas "inferida".
    """
    fingerprint = source_fingerprint(day, settings)
    rows = rows_from_attempts(list_files(attempts_dir(day, settings), "*.json.gz"), settings)
    first = min((r["TentativaEm"] for r in rows), default=None)
    rows += [
        r for r in rows_from_positions(day, settings) if first is None or r["TentativaEm"] < first
    ]
    if not rows:
        return None
    rows.sort(key=lambda r: r["TentativaEm"])
    table = pa.Table.from_pylist(rows, schema=coleta_schema(settings.timezone))
    table = table.replace_schema_metadata({FONTE_KEY: fingerprint})
    return write_parquet(table, coleta_path(day, settings))


def days_with_data(settings: Settings) -> list[date]:
    days = set()
    for dataset in ((BRONZE, COLETA_DATASET), (SILVER, DATASET)):
        for directory in (settings.data_dir.joinpath(*dataset)).glob("year=*/month=*/day=*"):
            try:
                days.add(date(*(int(part.split("=")[1]) for part in directory.parts[-3:])))
            except ValueError:
                continue
    return sorted(days)


def build_pending_cobertura(settings: Settings) -> list[Path]:
    """Dias sem cobertura ou cujas tentativas/posições mudaram desde a última vez."""
    return [
        path
        for day in days_with_data(settings)
        if stored_fingerprint(coleta_path(day, settings)) != source_fingerprint(day, settings)
        and (path := build_cobertura(day, settings)) is not None
    ]


def covered(attempts: pa.Table, at: datetime) -> bool:
    """Houve coleta válida a até COBERTURA_MINUTOS de `at`?"""
    window = timedelta(minutes=COBERTURA_MINUTOS)
    ok = attempts.filter(pc.is_in(attempts["Status"], pa.array(COBERTA)))
    times = ok["TentativaEm"]
    return bool(
        pc.any(
            pc.and_(pc.greater_equal(times, at - window), pc.less_equal(times, at + window))
        ).as_py()
    )
