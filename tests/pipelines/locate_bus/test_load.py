import pandas as pd
import pytest

from rmtc_go.core.storage import list_files, read_json, write_json_new
from rmtc_go.pipelines.locate_bus.extract import bronze_dir, payload_hash
from rmtc_go.pipelines.locate_bus.load import NoDataError, bronze_to_silver, quarantine_dir
from tests.conftest import DAY, collect, make_bus, make_payload


def read_silver(result) -> pd.DataFrame:
    return pd.read_parquet(result.path)


def test_bronze_to_silver(settings):
    collect(settings, make_payload(make_bus(1002), make_bus(2000, lat=0)), minute=11)
    collect(settings, make_payload(make_bus(1002)), minute=17)
    collect(settings, make_payload(status="erro"), minute=30)

    result = bronze_to_silver(DAY, settings)

    # load é 1:1 com o bronze: sem dedup, um parquet por coleta (vazio se nada válido)
    assert (result.files, result.skipped, result.rows, result.rejected) == (3, 0, 2, 2)
    assert [p.name for p in list_files(result.path, "*.parquet")] == [
        "20261022T091120.parquet",
        "20261022T091720.parquet",
        "20261022T093020.parquet",
    ]
    df = read_silver(result)
    assert df["Numero"].tolist() == [1002, 1002]
    assert {"LinhaNumero", "DestinoCurto", "ColetadoEm"} <= set(df.columns)
    assert str(df["ColetadoEm"].dt.tz) == "America/Sao_Paulo"

    quarantine = list_files(quarantine_dir(DAY, settings), "*.json")
    assert [p.name for p in quarantine] == ["20261022T091120.json", "20261022T093020.json"]


def test_bronze_to_silver_incremental(settings):
    collect(settings, make_payload(make_bus(1002)), minute=11)
    assert bronze_to_silver(DAY, settings).files == 1

    collect(settings, make_payload(make_bus(1003)), minute=17)
    result = bronze_to_silver(DAY, settings)
    assert (result.files, result.skipped, result.rows) == (1, 1, 1)
    assert read_silver(result)["Numero"].tolist() == [1002, 1003]


def test_bronze_to_silver_full_idempotente(settings):
    collect(settings, make_payload(make_bus(1002), make_bus(1003, lat=-16.7)))
    first = bronze_to_silver(DAY, settings)
    df_first = read_silver(first)

    second = bronze_to_silver(DAY, settings, full=True)
    assert (second.files, second.rows) == (first.files, first.rows)
    pd.testing.assert_frame_equal(df_first, read_silver(second))
    assert len(list_files(second.path)) == 1  # nenhum tmp sobrando


def test_bronze_to_silver_full_limpa_quarentena(settings):
    path = collect(settings, make_payload(make_bus(1002, lat=0)))
    bronze_to_silver(DAY, settings)
    assert list_files(quarantine_dir(DAY, settings))

    path.unlink()  # simula correção do bruto
    collect(settings, make_payload(make_bus(1002)))
    assert bronze_to_silver(DAY, settings, full=True).rejected == 0
    assert list_files(quarantine_dir(DAY, settings)) == []


def test_bronze_to_silver_le_json_sem_compressao(settings):
    record = read_json(collect(settings, make_payload(make_bus(1002)), minute=11))
    write_json_new(record, bronze_dir(DAY, settings) / "20261022T090000.json")
    assert bronze_to_silver(DAY, settings).rows == 2


def test_bronze_to_silver_hash_da_coleta(settings):
    payload = make_payload(make_bus(1002), make_bus(1003))
    collect(settings, payload, minute=11)
    collect(settings, payload, minute=13)
    collect(settings, make_payload(make_bus(1002, lat=-16.7)), minute=15)

    df = read_silver(bronze_to_silver(DAY, settings))
    assert df["ColetaSha256"].nunique() == 2  # as duas primeiras coletas são iguais
    assert set(df["ColetaSha256"]) >= {payload_hash(payload)}


def test_bronze_to_silver_recalcula_hash_de_coleta_antiga(settings):
    payload = make_payload(make_bus(1002))
    path = collect(settings, payload)
    record = read_json(path)
    del record["metadata"]["payload_sha256"]  # coleta gravada antes do hash existir
    path.unlink()
    write_json_new(record, path)

    assert read_silver(bronze_to_silver(DAY, settings))["ColetaSha256"].tolist() == [
        payload_hash(payload)
    ]


def test_bronze_to_silver_gzip_corrompido_vai_para_quarentena(settings):
    path = bronze_dir(DAY, settings) / "20261022T090000.json.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"lixo")
    assert bronze_to_silver(DAY, settings).rejected == 1


def test_bronze_to_silver_rejeita_kml_de_outro_dominio(settings):
    linha = {"LinhaNumero": "116", "LinhaKMLIda": "https://outro.com/a.kml", "LinhaKMLVolta": "x"}
    collect(settings, make_payload(make_bus(1002), make_bus(1003, Linha=linha)))
    assert bronze_to_silver(DAY, settings).rejected == 1


def test_bronze_to_silver_sem_dados(settings):
    with pytest.raises(NoDataError):
        bronze_to_silver(DAY, settings)


def test_bronze_to_silver_aceita_destino_e_kml_null(settings):
    sem_destino = make_bus(1002, Destino={"DestinoCurto": None})
    linha = {"LinhaNumero": "116", "LinhaKMLIda": None, "LinhaKMLVolta": None}
    sem_kml = make_bus(1003, Linha=linha)
    collect(settings, make_payload(sem_destino, sem_kml))

    result = bronze_to_silver(DAY, settings)
    assert (result.rows, result.rejected) == (2, 0)
    df = read_silver(result)
    assert df["DestinoCurto"].isna().tolist() == [True, False]
    assert df["LinhaKMLIda"].isna().tolist() == [False, True]


def test_bronze_to_silver_rejeita_chave_ausente_ou_destino_curto(settings):
    sem_chave = make_bus(1002, Destino={})
    curto = make_bus(1003, Destino={"DestinoCurto": "AB"})
    collect(settings, make_payload(sem_chave, curto, make_bus(1004)))
    assert bronze_to_silver(DAY, settings).rejected == 2
