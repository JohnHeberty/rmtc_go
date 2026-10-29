from datetime import datetime

import pyarrow.parquet as pq
import pytest

from rmtc_go.pipelines.locate_bus import pipeline
from rmtc_go.pipelines.locate_bus.coleta import (
    attempts_dir,
    build_cobertura,
    build_pending_cobertura,
    covered,
    save_attempt,
)
from rmtc_go.pipelines.locate_bus.extract import ExtractError
from rmtc_go.pipelines.locate_bus.load import bronze_to_silver as load_positions
from tests.conftest import DAY, TZ, collect, make_bus, make_payload
from tests.pipelines.locate_bus.test_extract import fake_session


def at(minute: int, hour: int = 9) -> datetime:
    return datetime(DAY.year, DAY.month, DAY.day, hour, minute, tzinfo=TZ)


@pytest.fixture
def api(monkeypatch):
    """extract com o relógio avançando 1 min por chamada e as respostas dadas em `api(...)`."""
    clock = iter(at(m) for m in range(60))

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(clock)

    def respond(*sessions):
        queue = iter(sessions)

        def factory(*args, **kwargs):
            session = next(queue)
            session.__enter__.return_value = session
            return session

        monkeypatch.setattr(pipeline, "build_session", factory)

    monkeypatch.setattr(pipeline, "datetime", FakeDatetime)
    return respond


def attempts(settings) -> list[str]:
    return pq.read_table(build_cobertura(DAY, settings))["Status"].to_pylist()


def test_extract_registra_toda_tentativa(settings, api):
    sem_kml = {"LinhaNumero": "116", "LinhaKMLIda": None, "LinhaKMLVolta": None}
    full = make_payload(make_bus(1002), make_bus(1003, Linha=sem_kml))
    api(
        fake_session(json_data=full),
        fake_session(json_data=full),
        fake_session(json_data=make_payload()),
        fake_session(status=500, text="fora"),
    )
    pipeline.extract(settings=settings)
    pipeline.extract(settings=settings)
    pipeline.extract(settings=settings)
    with pytest.raises(ExtractError):  # a falha continua subindo para o Prefect
        pipeline.extract(settings=settings)

    table = pq.read_table(build_cobertura(DAY, settings))
    assert table["Status"].to_pylist() == ["gravada", "igual", "vazia", "erro"]
    assert table["Onibus"].to_pylist() == [2, 2, 0, 0]
    assert table["OnibusSemKml"].to_pylist()[0] == 1
    assert "500" in table["Erro"].to_pylist()[3]


def test_falha_no_registro_nao_derruba_a_coleta(settings, api, monkeypatch):
    api(fake_session(json_data=make_payload(make_bus())))

    def broken(*args, **kwargs):
        raise OSError("disco cheio")

    monkeypatch.setattr(pipeline, "save_attempt", broken)
    assert pipeline.extract(settings=settings) is not None


def test_cobertura_inferida_das_posicoes_antes_do_registro(settings):
    for minute in (0, 1):
        collect(settings, make_payload(make_bus(1002)), minute=minute, second=0)
    load_positions(DAY, settings)
    save_attempt(at(5), "gravada", settings, onibus=1)
    assert attempts(settings) == ["inferida", "inferida", "gravada"]


def test_cobertura_refaz_so_quando_muda(settings):
    save_attempt(at(0), "gravada", settings, onibus=1)
    assert len(build_pending_cobertura(settings)) == 1
    assert build_pending_cobertura(settings) == []
    save_attempt(at(1), "igual", settings, onibus=1)
    assert len(build_pending_cobertura(settings)) == 1


def test_covered_ignora_vazia_e_erro(settings):
    save_attempt(at(0), "gravada", settings, onibus=1)
    save_attempt(at(10), "vazia", settings)
    save_attempt(at(20), "erro", settings, erro="fora")
    table = pq.read_table(build_cobertura(DAY, settings))
    assert covered(table, at(1))
    assert not covered(table, at(10)) and not covered(table, at(20))
    assert attempts_dir(DAY, settings).parts[-5:-3] == ("bronze", "locate_bus_coleta")
