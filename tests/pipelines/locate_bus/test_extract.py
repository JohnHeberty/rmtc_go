from datetime import datetime
from unittest.mock import MagicMock

import pytest
from requests import HTTPError

from rmtc_go.core.storage import read_json
from rmtc_go.pipelines.locate_bus import pipeline
from rmtc_go.pipelines.locate_bus.extract import (
    ExtractError,
    fetch,
    payload_hash,
    save_raw,
    source_url,
)
from tests.conftest import TZ, URL, collect, make_bus, make_payload


def fake_session(status: int = 200, json_data=None, text: str = "") -> MagicMock:
    response = MagicMock(status_code=status, text=text)
    response.raise_for_status.side_effect = HTTPError() if status >= 400 else None
    if json_data is None:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = json_data
    session = MagicMock()
    session.get.return_value = response
    return session


def test_fetch_ok():
    session = fake_session(json_data={"status": "sucesso", "onibus": []})
    assert fetch(session, "https://x/index.php", "116") == {"status": "sucesso", "onibus": []}
    assert session.get.call_args.kwargs["params"]["linha"] == "116"


def test_fetch_erro_http():
    with pytest.raises(ExtractError, match="500"):
        fetch(fake_session(status=500, text="boom"), "https://x")


def test_fetch_resposta_nao_json():
    with pytest.raises(ExtractError, match="não é JSON"):
        fetch(fake_session(text="<html>"), "https://x")


def test_fetch_resposta_nao_dict():
    with pytest.raises(ExtractError, match="formato inesperado"):
        fetch(fake_session(json_data=[1, 2]), "https://x")


def test_source_url(settings):
    assert source_url(settings) == "https://www.rmtcgoiania.com.br/index.php"


def test_save_raw_grava_payload_intacto(settings):
    payload = make_payload(make_bus(), make_bus(lat=0))  # lat inválida também vai pro bronze
    path = collect(settings, payload)

    assert path.parts[-4:] == ("year=2026", "month=10", "day=22", "20261022T091720.json.gz")
    record = read_json(path)
    assert record["payload"] == payload
    assert record["metadata"]["collected_at"] == "2026-10-22T09:17:20-03:00"


def test_save_raw_bronze_imutavel(settings):
    collect(settings, make_payload(make_bus()))
    with pytest.raises(FileExistsError):
        collect(settings, make_payload(make_bus()))


def test_save_raw_exige_timezone(settings):
    with pytest.raises(ValueError, match="timezone"):
        save_raw({}, datetime(2026, 10, 22), "000", URL, settings)


def test_payload_hash_ignora_ordem_das_chaves():
    assert payload_hash({"a": 1, "b": [2]}) == payload_hash({"b": [2], "a": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_save_raw_grava_hash_do_payload(settings):
    payload = make_payload(make_bus())
    first = read_json(collect(settings, payload, second=0))
    second = read_json(collect(settings, payload, second=1))
    # mesmo dado em coletas diferentes: arquivos diferentes, hash do payload igual
    assert first["metadata"]["payload_sha256"] == payload_hash(payload)
    assert first["metadata"]["payload_sha256"] == second["metadata"]["payload_sha256"]


def test_extract_avisa_lista_vazia(settings, monkeypatch, caplog):
    session = fake_session(json_data=make_payload())
    session.__enter__.return_value = session
    monkeypatch.setattr(pipeline, "build_session", lambda *a, **k: session)

    pipeline.extract(settings=settings)
    assert "sem nenhum ônibus" in caplog.text


def test_extract_nao_grava_coleta_igual_a_anterior(settings, monkeypatch):
    payloads = iter([make_payload(make_bus(1002))] * 2 + [make_payload(make_bus(1003))])

    def session_factory(*args, **kwargs):
        session = fake_session(json_data=next(payloads))
        session.__enter__.return_value = session
        return session

    clock = iter(datetime(2026, 10, 22, 9, minute, tzinfo=TZ) for minute in range(0, 10, 2))

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(clock)

    monkeypatch.setattr(pipeline, "build_session", session_factory)
    monkeypatch.setattr(pipeline, "datetime", FakeDatetime)
    first = pipeline.extract(settings=settings)
    assert pipeline.extract(settings=settings) is None  # igual: não grava
    third = pipeline.extract(settings=settings)
    assert first is not None and third is not None and first != third
