from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq

from rmtc_go.pipelines.linha_kml.load import bronze_to_silver
from rmtc_go.pipelines.linha_kml.tracado import (
    Segment,
    build_tracado,
    segments,
    tracado_em,
    tracado_path,
)
from rmtc_go.pipelines.linha_kml.uso import build_pending
from rmtc_go.pipelines.locate_bus.coleta import build_pending_cobertura, save_attempt
from rmtc_go.pipelines.locate_bus.extract import save_raw
from rmtc_go.pipelines.locate_bus.load import bronze_to_silver as load_positions
from tests.conftest import DAY, TZ, URL, collect, make_bus, make_payload
from tests.pipelines.linha_kml.conftest import at
from tests.pipelines.linha_kml.test_linha_kml import BASE, ROTA_A, ROTA_B, run

LM_2023 = "Wed, 10 May 2023 20:23:27 GMT"
DESDE_2023 = datetime(2023, 5, 10, 20, 23, 27, tzinfo=UTC)


def t(hour: int, minute: int = 0, day: int = DAY.day) -> datetime:
    return datetime(DAY.year, DAY.month, day, hour, minute, tzinfo=TZ)


def version(n, precisao, desde, troca_apos, ultima):
    return {
        "Versao": n,
        "Precisao": precisao,
        "ValidoDesde": desde,
        "TrocaApos": troca_apos,
        "UltimaObs": ultima,
    }


def test_segments_versao_unica_com_last_modified():
    assert segments([version(1, "exata", DESDE_2023, None, at(8))]) == [
        Segment(None, DESDE_2023, 1, "anterior"),
        Segment(DESDE_2023, at(8), 1, "certo"),
        Segment(at(8), None, 1, "nao_confirmado"),
    ]


def test_segments_troca_sem_last_modified_vira_janela():
    v1 = version(1, "desde_1a_obs", at(5), None, at(7))
    v2 = version(2, "janela", at(8), at(7), at(9))
    assert segments([v1, v2])[1:4] == [
        Segment(at(5), at(7), 1, "certo"),
        Segment(at(7), at(8), 2, "janela"),  # pode ser a 1 ou a 2
        Segment(at(8), at(9), 2, "certo"),
    ]


def test_segments_troca_exata_nao_tem_janela():
    v1 = version(1, "exata", DESDE_2023, None, at(7))
    v2 = version(2, "exata", t(7, 30), at(7), at(9))
    assert [s.status for s in segments([v1, v2])] == [
        "anterior",
        "certo",
        "certo",
        "nao_confirmado",
    ]
    assert segments([v1, v2])[1].end == t(7, 30)


def positions(settings, hour: int, minutes, **bus):
    for minute in minutes:
        collect(settings, make_payload(make_bus(1002, **bus)), hour=hour, minute=minute)
    load_positions(DAY, settings)


def rebuild(settings):
    bronze_to_silver(settings)
    build_pending(settings)
    build_pending_cobertura(settings)
    return build_tracado(settings)


def test_tracado_resolve_versao_e_consulta(settings, clock):
    for _ in range(4):  # downloads às 05h..08h, sem mudança desde 2023
        run(settings, [(ROTA_A, LM_2023)])
    positions(settings, 7, range(5))
    rebuild(settings)

    r = tracado_em("116", "ida", t(7, 2), settings)
    assert (r.status, r.arquivo, r.versao) == ("certo", "linha-116ida.kml", 1)
    # a volta nunca foi baixada
    assert tracado_em("116", "volta", t(7, 2), settings).status == "sem_download"


def test_tracado_fora_do_uso_separa_sem_coleta_de_sem_uso(settings, clock):
    run(settings, [(ROTA_A, LM_2023)])
    positions(settings, 7, range(3))
    outra = {"LinhaNumero": "117", "LinhaKMLIda": BASE + "x117ida.kml", "LinhaKMLVolta": None}
    positions(settings, 7, [30], Linha=outra)  # havia coleta às 07:30, sem a linha 116
    rebuild(settings)

    assert tracado_em("116", "ida", t(7, 30), settings).status == "sem_uso"
    assert tracado_em("116", "ida", t(12), settings).status == "sem_coleta"
    save_attempt(t(12), "igual", settings, onibus=1)  # coleta igual à anterior: coberta
    build_pending_cobertura(settings)
    assert tracado_em("116", "ida", t(12), settings).status == "sem_uso"


def test_tracado_kml_null(settings, clock):
    run(settings, [(ROTA_A, LM_2023)])
    sem_kml = {"LinhaNumero": "116", "LinhaKMLIda": None, "LinhaKMLVolta": None}
    positions(settings, 4, range(3), Linha=sem_kml)
    rebuild(settings)
    r = tracado_em("116", "ida", t(4, 1), settings)
    assert (r.status, r.arquivo) == ("sem_kml", None)


def test_tracado_corta_periodo_na_troca_de_versao(settings, clock):
    run(settings, [(ROTA_A, LM_2023)])  # 05h
    run(settings, [(ROTA_B, "Thu, 22 Oct 2026 09:30:00 GMT")])  # 06h; mudou às 06:30 (-03)?
    for hour, minute in [(4, 50), (5, 0), (5, 10)]:  # o período atravessa as 05h
        collect(settings, make_payload(make_bus(1002)), hour=hour, minute=minute)
    load_positions(DAY, settings)
    rebuild(settings)
    table = pq.read_table(tracado_path(DAY, settings)).to_pylist()
    ida = [r for r in table if r["Sentido"] == "ida"]
    # Last-Modified 06:30 depois da 1ª obs de B (06h): incoerente, cai na janela 05h..06h
    assert [(r["Versao"], r["Status"]) for r in ida] == [(1, "certo"), (2, "janela")]
    assert ida[0]["Ate"] == ida[1]["Desde"] == at(5)


def test_tracado_arquivo_anterior_atravessa_a_meia_noite(settings, clock):
    def linha(arquivo):
        return {
            "LinhaNumero": "004",
            "LinhaKMLIda": f"{BASE}{arquivo}-004ida.kml",
            "LinhaKMLVolta": f"{BASE}{arquivo}-004volta.kml",
        }

    noite = t(23, 58)
    for i, eixo in enumerate(["Eixo85", "Eixo85", "Eixo85", "EixoT9"]):  # 23:58..00:01
        moment = noite + timedelta(minutes=i)
        save_raw(make_payload(make_bus(1, Linha=linha(eixo))), moment, "000", URL, settings)
    load_positions(DAY, settings)
    load_positions(DAY + timedelta(days=1), settings)
    rebuild(settings)

    proximo = pq.read_table(tracado_path(DAY + timedelta(days=1), settings)).to_pylist()
    ida = [(r["Arquivo"], r["ArquivoAnterior"]) for r in proximo if r["Sentido"] == "ida"]
    # o corte da meia-noite não é troca; a troca é às 00:01
    assert ida == [
        ("Eixo85-004ida.kml", "Eixo85-004ida.kml"),
        ("EixoT9-004ida.kml", "Eixo85-004ida.kml"),
    ]
