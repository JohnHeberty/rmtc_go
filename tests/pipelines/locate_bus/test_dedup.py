from rmtc_go.core.storage import file_stem, list_files
from rmtc_go.pipelines.locate_bus.dedup import remove_duplicates
from rmtc_go.pipelines.locate_bus.extract import bronze_dir, bronze_files
from rmtc_go.pipelines.locate_bus.load import bronze_to_silver, quarantine_dir, silver_dir
from tests.conftest import DAY, collect, make_bus, make_payload

A = make_payload(make_bus(1002))
B = make_payload(make_bus(1002, lat=-16.7))
VAZIO = make_payload()


def stems(paths) -> list[str]:
    return [file_stem(p) for p in paths]


def test_remove_coletas_iguais_a_anterior(settings):
    for minute, payload in enumerate([A, A, B, VAZIO, VAZIO, VAZIO, A]):
        collect(settings, payload, minute=minute)
    bronze_to_silver(DAY, settings)

    result = remove_duplicates(DAY, settings)

    # mantém a primeira de cada sequência; A volta no fim depois de outro payload e fica
    kept = ["20261022T090020", "20261022T090220", "20261022T090320", "20261022T090620"]
    assert result.removed == ["20261022T090120", "20261022T090420", "20261022T090520"]
    assert stems(bronze_files(DAY, settings)) == kept
    assert stems(list_files(silver_dir(DAY, settings), "*.parquet")) == kept


def test_remove_quarentena_da_duplicata(settings):
    invalido = make_payload(make_bus(1002, lat=0))
    collect(settings, invalido, minute=1)
    collect(settings, invalido, minute=2)
    bronze_to_silver(DAY, settings)
    assert len(list_files(quarantine_dir(DAY, settings))) == 2

    remove_duplicates(DAY, settings)
    assert stems(list_files(quarantine_dir(DAY, settings))) == ["20261022T090120"]


def test_dedup_sem_duplicatas_nao_apaga_nada(settings):
    collect(settings, A, minute=1)
    collect(settings, B, minute=2)
    assert remove_duplicates(DAY, settings).removed == []
    assert len(bronze_files(DAY, settings)) == 2


def test_dedup_ignora_arquivo_corrompido(settings):
    collect(settings, A, minute=1)
    (bronze_dir(DAY, settings) / "20261022T090130.json.gz").write_bytes(b"lixo")
    collect(settings, A, minute=2)

    # o corrompido não conta como "anterior": a coleta das 9:02 é igual à das 9:01
    assert remove_duplicates(DAY, settings).removed == ["20261022T090220"]
    assert "20261022T090130" in stems(bronze_files(DAY, settings))


def test_dedup_dia_vazio(settings):
    assert remove_duplicates(DAY, settings).removed == []
