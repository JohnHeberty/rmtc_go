import gzip
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import IO, Any

import pyarrow as pa
import pyarrow.parquet as pq

BRONZE = "bronze"
SILVER = "silver"
QUARANTINE = "quarantine"


def partition_dir(root: Path, layer: str, dataset: str, day: date) -> Path:
    """Diretório da partição no padrão Hive: <root>/<layer>/<dataset>/year=YYYY/month=MM/day=DD."""
    return root / layer / dataset / f"year={day:%Y}" / f"month={day:%m}" / f"day={day:%d}"


def list_files(directory: Path, *patterns: str) -> list[Path]:
    """Arquivos da pasta em ordem alfabética; lista vazia se a pasta não existir."""
    if not directory.is_dir():
        return []
    files = {p for pattern in patterns or ("*",) for p in directory.glob(pattern) if p.is_file()}
    return sorted(files)


def file_stem(path: Path) -> str:
    """Nome sem nenhuma extensão: 20261022T091720.json.gz -> 20261022T091720."""
    return path.name.split(".", 1)[0]


@contextmanager
def atomic_path(path: Path) -> Iterator[Path]:
    """Caminho temporário único na mesma pasta, movido para `path` ao final (tmp + replace).

    O nome único evita que duas execuções simultâneas escrevam no mesmo tmp.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        yield tmp
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _open_text(path: Path, mode: str, gz: bool | None = None) -> IO[str]:
    """Abre em modo texto, com gzip transparente para arquivos .gz."""
    if path.suffix == ".gz" if gz is None else gz:
        return gzip.open(path, f"{mode}t", encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def write_json_new(data: Any, path: Path) -> Path:
    """Grava JSON (gzip se terminar em .gz) sem nunca sobrescrever: o bronze é imutável.

    FileExistsError se já existir.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text(path, "x") as f:
        json.dump(data, f, ensure_ascii=False)
    return path


def write_json(data: Any, path: Path) -> Path:
    """Grava JSON de forma atômica, substituindo o arquivo se existir."""
    with atomic_path(path) as tmp, _open_text(tmp, "w", gz=path.suffix == ".gz") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    return path


def read_json(path: Path) -> Any:
    with _open_text(path, "r") as f:
        return json.load(f)


def write_parquet(table: pa.Table, path: Path) -> Path:
    """Grava parquet (zstd) de forma atômica, substituindo a versão anterior.

    Relê o rodapé do arquivo temporário antes da troca: se não tiver todas as linhas,
    a versão anterior fica intacta.
    """
    with atomic_path(path) as tmp:
        pq.write_table(table, tmp, compression="zstd")
        written = pq.read_metadata(tmp).num_rows
        if written != table.num_rows:
            raise OSError(f"{path.name}: gravou {written} de {table.num_rows} linhas")
    return path


def parquet_rows(paths: list[Path]) -> int:
    """Total de linhas lendo só o rodapé de cada parquet (não carrega os dados)."""
    return sum(pq.read_metadata(p).num_rows for p in paths)
