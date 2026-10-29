import gzip
from datetime import date
from pathlib import Path

import pytest

from rmtc_go.core.storage import (
    file_stem,
    list_files,
    partition_dir,
    read_json,
    write_json,
    write_json_new,
)


def test_partition_dir_usa_padrao_hive(tmp_path: Path):
    path = partition_dir(tmp_path, "bronze", "locate_bus", date(2026, 1, 5))
    assert path == tmp_path / "bronze" / "locate_bus" / "year=2026" / "month=01" / "day=05"


def test_write_json_new_nao_sobrescreve(tmp_path: Path):
    path = tmp_path / "a" / "x.json"
    write_json_new({"v": 1}, path)
    with pytest.raises(FileExistsError):
        write_json_new({"v": 2}, path)
    assert read_json(path) == {"v": 1}


def test_write_json_substitui(tmp_path: Path):
    path = tmp_path / "x.json"
    write_json({"v": 1}, path)
    write_json({"v": 2}, path)
    assert read_json(path) == {"v": 2}
    assert list_files(tmp_path) == [path]


def test_json_gz_ida_e_volta(tmp_path: Path):
    path = write_json_new({"ônibus": 1}, tmp_path / "x.json.gz")
    assert gzip.decompress(path.read_bytes())  # é gzip de verdade
    assert read_json(path) == {"ônibus": 1}
    write_json({"v": 2}, tmp_path / "y.json.gz")
    assert read_json(tmp_path / "y.json.gz") == {"v": 2}


def test_file_stem():
    assert file_stem(Path("20261022T091720.json.gz")) == "20261022T091720"


def test_list_files_pasta_inexistente(tmp_path: Path):
    assert list_files(tmp_path / "nao-existe") == []
