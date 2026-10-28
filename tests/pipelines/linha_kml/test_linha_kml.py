from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from requests import ConnectionError as RequestsConnectionError

from rmtc_go.core.storage import read_json
from rmtc_go.pipelines.linha_kml import extract as extract_module
from rmtc_go.pipelines.linha_kml.dedup import remove_duplicates
from rmtc_go.pipelines.linha_kml.extract import (
    ExtractError,
    Target,
    discover_targets,
    extract_targets,
    fetch_dir,
    fetch_kml,
    latest_hash,
    save_fetch_log,
    save_version,
    strip_query,
    target_dir,
    version_files,
)
from rmtc_go.pipelines.linha_kml.load import (
    SilverShrinkError,
    bronze_to_silver,
    parse_kml,
    quarantine_path,
)
from rmtc_go.pipelines.linha_kml.pipeline import discovery_days
from rmtc_go.pipelines.linha_kml.uso import build_pending, build_uso, uso_path
from rmtc_go.pipelines.locate_bus.extract import save_raw
from rmtc_go.pipelines.locate_bus.load import bronze_to_silver as load_positions
from tests.conftest import DAY, KML, TZ, URL, collect, make_bus, make_payload

URL_IDA = strip_query(KML.format("ida"))
T_IDA = Target(linha="116", sentido="ida", url=URL_IDA)
BASE = "https://www.rmtcgoiania.com.br/components/com_rmtclinhas/satelite/"
T_85 = Target("004", "ida", BASE + "Eixo85-004ida.kml")
T_T9 = Target("004", "ida", BASE + "EixoT9-004ida.kml")


def kml(*lines: str, name: str = "116 - IDA") -> str:
    placemarks = "".join(
        f"<Placemark><name>{name}</name><LineString><coordinates>{c}</coordinates>"
        "</LineString></Placemark>"
        for c in lines
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<kml xmlns="http://www.opengis.net/kml/2.2"><Document>{placemarks}</Document></kml>'
    )


ROTA_A = kml("-49.2,-16.6,0 -49.3,-16.7,0")
ROTA_A_REFORMATADA = kml("-49.2,-16.6,0   -49.3,-16.7,0", name="116 - IDA ")  # mesmo traçado
ROTA_B = kml("-49.2,-16.6,0 -49.4,-16.8,0")


def fake_session(*bodies) -> MagicMock:
    responses = []
    for body in bodies:
        if isinstance(body, Exception):
            responses.append(body)
            continue
        r = MagicMock(status_code=200, content=body.encode())
        r.raise_for_status.return_value = None
        responses.append(r)
    session = MagicMock()
    session.get.side_effect = responses
    return session


def at(hour: int, day: int = DAY.day) -> datetime:
    return datetime(DAY.year, DAY.month, day, hour, tzinfo=TZ)


@pytest.fixture
def clock(monkeypatch):
    """Relógio do extract avançando 1 h a cada chamada (evita duas versões no mesmo segundo)."""
    times = iter(at(h % 24, DAY.day + h // 24) for h in range(5, 200))

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(times)

    monkeypatch.setattr(extract_module, "datetime", FakeDatetime)


def run(settings, bodies, targets=(T_IDA,)):
    """Uma execução do extract (com registro _fetch), como o pipeline faz."""
    result = extract_targets(fake_session(*bodies), list(targets), settings)
    save_fetch_log(result.entries, [DAY], result.entries[0].fetched_at, settings)
    return result


def versao(settings) -> pd.DataFrame:
    return pd.read_parquet(bronze_to_silver(settings).path_versao)


# ---------- extract ----------


def test_strip_query_remove_rev():
    assert strip_query(KML.format("ida") + "?rev=20261027-13") == URL_IDA


def test_discover_targets_do_silver_de_posicoes(settings):
    sem_kml = {"LinhaNumero": "200", "LinhaKMLIda": None, "LinhaKMLVolta": None}
    collect(settings, make_payload(make_bus(1002), make_bus(1004, Linha=sem_kml)))
    load_positions(DAY, settings)
    targets = discover_targets([DAY], settings)
    assert [(t.linha, t.sentido) for t in targets] == [("116", "ida"), ("116", "volta")]
    assert targets[0].url == URL_IDA


def test_discover_targets_junta_varios_dias(settings):
    collect(settings, make_payload(make_bus(1002)))
    load_positions(DAY, settings)
    outra = {"LinhaNumero": "117", "LinhaKMLIda": KML.format("ida"), "LinhaKMLVolta": None}
    vespera = datetime(2026, 10, 21, 9, tzinfo=TZ)
    save_raw(make_payload(make_bus(2000, Linha=outra)), vespera, "000", URL, settings)
    load_positions(vespera.date(), settings)
    assert {t.linha for t in discover_targets(discovery_days(DAY), settings)} == {"116", "117"}


def test_discovery_days_inclui_hoje_e_seis_anteriores():
    days = discovery_days(DAY)
    assert len(days) == 7 and days[-1] == DAY and days[0] == date(2026, 10, 16)


def test_fetch_kml_rejeita_html():
    with pytest.raises(ExtractError, match="não é KML"):
        fetch_kml(fake_session("<html>erro</html>"), URL_IDA)


def test_extract_grava_so_quando_muda_mas_registra_toda_tentativa(settings, clock):
    for body in (ROTA_A, ROTA_A, ROTA_B):
        run(settings, [body])
    assert len(version_files(target_dir(T_IDA, settings))) == 2
    logs = sorted(fetch_dir(settings).glob("*.json.gz"))
    entries = [read_json(p)["entries"][0] for p in logs]
    assert [e["status"] for e in entries] == ["ok", "ok", "ok"]
    assert [e["salvo"] is not None for e in entries] == [True, False, True]


def test_extract_registra_falha_e_segue(settings, clock):
    t_volta = Target("116", "volta", URL_IDA.replace("ida", "volta"))
    result = run(settings, [RequestsConnectionError("fora"), ROTA_A], targets=(T_IDA, t_volta))
    assert result.failed == ["116/ida/linha-116ida"]
    assert [e.status for e in result.entries] == ["erro", "ok"]
    assert "fora" in result.entries[0].erro


def test_latest_hash_pula_versao_ilegivel(settings):
    save_version(T_IDA, ROTA_A, at(5), settings)
    (target_dir(T_IDA, settings) / "20261022T060000.json.gz").write_bytes(b"lixo")
    # antes: None -> regravava A, o dedup apagava, e isso se repetia todo dia
    assert latest_hash(T_IDA, settings) == extract_module.content_hash(ROTA_A)


# ---------- kml_versao / kml_observacao ----------


def test_parse_kml_linestring_e_multi():
    name, wkt, n = parse_kml(ROTA_A)
    assert (name, wkt, n) == ("116 - IDA", "LINESTRING (-49.2 -16.6, -49.3 -16.7)", 2)
    _, wkt, n = parse_kml(kml("-49.2,-16.6 -49.3,-16.7", "-49.5,-16.9 -49.6,-17.0"))
    assert wkt.startswith("MULTILINESTRING ((-49.2 -16.6") and n == 4


def test_parse_kml_sem_linha():
    with pytest.raises(ValueError):
        parse_kml(kml())


def test_versao_com_janela_da_troca(settings, clock):
    for body in (ROTA_A, ROTA_A, ROTA_A, ROTA_B, ROTA_B):  # 05h..09h
        run(settings, [body])
    df = versao(settings)
    assert df["Versao"].tolist() == [1, 2]
    a, b = df.iloc[0], df.iloc[1]
    assert (a.PrimeiraObs, a.UltimaObs, a.Observacoes) == (at(5), at(7), 3)
    assert pd.isna(a.TrocaApos) and a.SubstituidaEm == at(8)
    # a troca aconteceu entre a última confirmação de A (07h) e a 1ª vez que vimos B (08h)
    assert (b.TrocaApos, b.PrimeiraObs, b.UltimaObs) == (at(7), at(8), at(9))
    assert pd.isna(b.SubstituidaEm)


def test_falha_de_download_nao_confirma_versao(settings, clock):
    run(settings, [ROTA_A])  # 05h
    run(settings, [RequestsConnectionError("fora")])  # 06h
    run(settings, [ROTA_B])  # 07h
    a, b = versao(settings).itertuples()
    assert a.UltimaObs == at(5)  # sem prova de que A valia às 06h
    assert b.TrocaApos == at(5)  # a janela da troca inclui a falha


def test_rota_que_volta_e_mudanca_real(settings, clock):
    for body in (ROTA_A, ROTA_B, ROTA_A):
        run(settings, [body])
    assert versao(settings)["Versao"].tolist() == [1, 2, 3]


def test_reformatacao_do_kml_nao_e_nova_versao(settings, clock):
    run(settings, [ROTA_A])
    run(settings, [ROTA_A_REFORMATADA])  # texto diferente, mesma geometria
    assert len(version_files(target_dir(T_IDA, settings))) == 2  # bronze guarda o bruto
    df = versao(settings)
    assert len(df) == 1 and df.iloc[0].Observacoes == 2


def test_observacoes_incluem_versoes_sem_registro(settings, clock):
    save_version(T_IDA, ROTA_A, at(4), settings)  # gravada antes de existir o _fetch
    run(settings, [ROTA_A])
    obs = pd.read_parquet(bronze_to_silver(settings).path_observacao)
    assert obs["Origem"].tolist() == ["versao", "fetch"]
    assert obs["Status"].tolist() == ["ok", "ok"]


def test_traçados_por_eixo_sao_independentes(settings, clock):
    run(settings, [ROTA_A, ROTA_B], targets=(T_85, T_T9))
    df = versao(settings)
    assert sorted(df["Arquivo"]) == ["Eixo85-004ida.kml", "EixoT9-004ida.kml"]
    assert df["Versao"].tolist() == [1, 1]


def test_kml_invalido_vai_para_quarentena(settings, clock):
    run(settings, [ROTA_A])
    save_version(Target("117", "ida", URL_IDA), "<kml>quebrado", at(5), settings)
    assert bronze_to_silver(settings).rejected == 1
    assert "linha=117" in read_json(quarantine_path(settings))[0]["file"]


def test_recusa_se_bronze_perdeu_dados(settings, clock):
    run(settings, [ROTA_A])
    run(settings, [ROTA_B])
    bronze_to_silver(settings)
    for p in fetch_dir(settings).glob("*.json.gz"):
        p.unlink()
    version_files(target_dir(T_IDA, settings))[0].unlink()
    with pytest.raises(SilverShrinkError):
        bronze_to_silver(settings)


# ---------- dedup ----------


def test_dedup_apaga_versao_igual_a_anterior(settings):
    save_version(T_IDA, ROTA_A, at(5), settings)
    save_version(T_IDA, ROTA_A, at(6), settings)
    save_version(T_IDA, ROTA_B, at(7), settings)
    save_version(T_IDA, ROTA_A, at(8), settings)  # voltou ao antigo: fica

    removed = remove_duplicates(settings).removed
    assert removed == ["linha=116/sentido=ida/arquivo=linha-116ida/20261022T060000.json.gz"]
    assert versao(settings)["Versao"].tolist() == [1, 2, 3]


def test_dedup_nao_mistura_tracados(settings):
    t_volta = Target("116", "volta", URL_IDA.replace("ida", "volta"))
    save_version(T_IDA, ROTA_A, at(5), settings)
    save_version(t_volta, ROTA_A, at(5), settings)
    assert remove_duplicates(settings).removed == []


def test_dedup_ignora_versao_ilegivel_e_pasta_fetch(settings, clock):
    run(settings, [ROTA_A])
    (target_dir(T_IDA, settings) / "20261022T053000.json.gz").write_bytes(b"lixo")
    save_version(T_IDA, ROTA_A, at(23), settings)
    run(settings, [ROTA_A])  # registro _fetch igual ao anterior: não é versão

    removed = remove_duplicates(settings).removed
    assert [Path(r).name for r in removed] == ["20261022T230000.json.gz"]
    assert (target_dir(T_IDA, settings) / "20261022T053000.json.gz").exists()
    assert len(list(fetch_dir(settings).glob("*.json.gz"))) == 2


# ---------- linha_kml_uso ----------


def linha_004(arquivo: str) -> dict:
    return {
        "LinhaNumero": "004",
        "LinhaKMLIda": f"{BASE}{arquivo}ida.kml?rev=20261022-04",
        "LinhaKMLVolta": f"{BASE}{arquivo}volta.kml?rev=20261022-04",
    }


def test_uso_registra_troca_de_kml_no_dia(settings):
    # 04:00-04:02 EixoT9; 04:03-04:04 Eixo85 (como a linha 004 em 28/10)
    for minute, eixo in [(0, "EixoT9-004"), (1, "EixoT9-004"), (2, "EixoT9-004"),
                         (3, "Eixo85-004"), (4, "Eixo85-004")]:  # fmt: skip
        collect(settings, make_payload(make_bus(1, Linha=linha_004(eixo))), hour=4, minute=minute)
    load_positions(DAY, settings)

    df = pd.read_parquet(build_uso(DAY, settings).path)
    ida = df[df.Sentido == "ida"]
    assert ida["Arquivo"].tolist() == ["EixoT9-004ida.kml", "Eixo85-004ida.kml"]
    assert ida["Coletas"].tolist() == [3, 2]
    assert ida.iloc[1].UsoDesde == datetime(2026, 10, 22, 4, 3, 20, tzinfo=TZ)


def test_uso_fecha_periodo_apos_lacuna(settings):
    for minute in (0, 1, 40):  # 39 min sem a linha
        collect(settings, make_payload(make_bus(1002)), hour=9, minute=minute)
    load_positions(DAY, settings)
    df = pd.read_parquet(build_uso(DAY, settings).path)
    assert df[df.Sentido == "ida"]["Coletas"].tolist() == [2, 1]


def test_uso_pendente_reconstroi_recentes_e_antigos_sem_uso(settings):
    collect(settings, make_payload(make_bus(1002)))
    load_positions(DAY, settings)
    assert [r.day for r in build_pending([], settings)] == [DAY]  # antigo sem uso
    assert build_pending([], settings) == []  # já tem: não refaz
    assert [r.day for r in build_pending([DAY], settings)] == [DAY]  # recente: refaz
    assert uso_path(DAY, settings).exists()


def test_uso_fica_dentro_da_pasta_da_linha_kml(settings):
    assert uso_path(DAY, settings).relative_to(settings.data_dir).as_posix() == (
        "silver/linha_kml/uso/year=2026/month=10/day=22/linha_kml_uso_20261022.parquet"
    )
