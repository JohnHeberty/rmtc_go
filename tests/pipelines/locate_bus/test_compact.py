from datetime import datetime

import pandas as pd
import pytest

from rmtc_go.core.storage import list_files
from rmtc_go.pipelines.locate_bus import pipeline
from rmtc_go.pipelines.locate_bus.compact import (
    SilverShrinkError,
    build_daily,
    compact_closed_days,
    days_to_compact,
)
from rmtc_go.pipelines.locate_bus.dedup import remove_duplicates
from rmtc_go.pipelines.locate_bus.extract import bronze_files, save_raw
from rmtc_go.pipelines.locate_bus.load import (
    bronze_to_silver,
    collection_parquets,
    daily_parquet,
    quarantine_dir,
    silver_dir,
)
from tests.conftest import DAY, TZ, URL, collect, make_bus, make_payload

A = make_payload(make_bus(1002), make_bus(1003))
B = make_payload(make_bus(1002, lat=-16.7), make_bus(2000, lat=0))  # 1 válido, 1 rejeitado


def coletar_dia(settings):
    collect(settings, A, minute=1)
    collect(settings, B, minute=2)
    collect(settings, make_payload(), minute=3)  # vazia
    bronze_to_silver(DAY, settings)  # incremental, como ao longo do dia


def test_build_daily_junta_tudo_num_arquivo(settings):
    coletar_dia(settings)
    antes = pd.read_parquet(silver_dir(DAY, settings))

    result = build_daily(DAY, settings)

    assert list_files(silver_dir(DAY, settings)) == [daily_parquet(DAY, settings)]
    assert (result.files, result.rows, result.rejected) == (3, 3, 1)
    depois = pd.read_parquet(result.path)
    pd.testing.assert_frame_equal(antes.reset_index(drop=True), depois)  # mesmas linhas
    assert len(list_files(quarantine_dir(DAY, settings))) == 1


def test_build_daily_idempotente(settings):
    coletar_dia(settings)
    first = pd.read_parquet(build_daily(DAY, settings).path)
    second = pd.read_parquet(build_daily(DAY, settings).path)
    pd.testing.assert_frame_equal(first, second)


def test_build_daily_recusa_se_bronze_perdeu_arquivo(settings):
    coletar_dia(settings)
    bronze_files(DAY, settings)[0].unlink()  # simula perda no bronze

    with pytest.raises(SilverShrinkError):
        build_daily(DAY, settings)
    assert len(collection_parquets(DAY, settings)) == 3  # silver intacto
    assert not daily_parquet(DAY, settings).exists()


def test_build_daily_recupera_coleta_nao_carregada(settings):
    coletar_dia(settings)
    collect(settings, make_payload(make_bus(3000)), minute=4)  # bronze sem silver (load falhou)
    assert build_daily(DAY, settings).rows == 4


def test_build_daily_termina_apos_queda_no_meio(settings):
    coletar_dia(settings)
    build_daily(DAY, settings)
    # queda depois de gravar o dia e antes de apagar os por coleta: sobram os dois
    bronze_to_silver(DAY, settings, full=True)
    assert len(collection_parquets(DAY, settings)) == 3

    assert build_daily(DAY, settings).rows == 3  # sem duplicar
    assert collection_parquets(DAY, settings) == []


def test_dedup_em_dia_compactado_reconstroi(settings):
    coletar_dia(settings)
    collect(settings, B, minute=5)  # igual à anterior? não: a anterior é a vazia
    collect(settings, B, minute=6)  # esta sim é duplicata
    build_daily(DAY, settings)
    assert pd.read_parquet(daily_parquet(DAY, settings)).shape[0] == 5

    assert remove_duplicates(DAY, settings).removed == ["20261022T090620"]
    assert pd.read_parquet(daily_parquet(DAY, settings)).shape[0] == 4


def test_compact_closed_days_ignora_hoje(settings):
    coletar_dia(settings)
    now = datetime.now(TZ)
    save_raw(make_payload(make_bus(1002)), now, "000", URL, settings)
    bronze_to_silver(now.date(), settings)

    assert days_to_compact(settings) == [DAY]
    assert [r.day for r in compact_closed_days(settings)] == [DAY]
    assert len(collection_parquets(now.date(), settings)) == 1  # hoje fica por coleta
    assert days_to_compact(settings) == []


def test_compact_closed_days_segue_se_um_dia_falha(settings, caplog):
    coletar_dia(settings)
    bronze_files(DAY, settings)[0].unlink()
    assert compact_closed_days(settings) == []
    assert "não feita" in caplog.text


def test_pipeline_load_de_dia_fechado_gera_arquivo_do_dia(settings):
    collect(settings, A, minute=1)
    result = pipeline.load(DAY, settings=settings)
    assert result.path == daily_parquet(DAY, settings)
    assert collection_parquets(DAY, settings) == []
