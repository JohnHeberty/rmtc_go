from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from rmtc_go.core.settings import Settings
from rmtc_go.pipelines.locate_bus.extract import save_raw

KML = "https://www.rmtcgoiania.com.br/components/com_rmtclinhas/satelite/linha-116{}.kml"
TZ = ZoneInfo("America/Sao_Paulo")
DAY = date(2026, 10, 22)
URL = "https://www.rmtcgoiania.com.br/index.php"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, data_dir=tmp_path)


def make_bus(numero: int = 1002, lat: float = -16.667, lon: float = -49.308, **overrides) -> dict:
    bus = {
        "Numero": numero,
        "Latitude": lat,
        "Longitude": lon,
        "Acessivel": 1,
        "Situacao": "Adiantado",
        "Linha": {
            "LinhaNumero": "116",
            "LinhaKMLIda": KML.format("ida"),
            "LinhaKMLVolta": KML.format("volta"),
        },
        "Destino": {"DestinoCurto": "T VERA CRUZ"},
    }
    bus.update(overrides)
    return bus


def make_payload(*buses: dict, status: str = "sucesso") -> dict:
    return {"status": status, "onibus": list(buses)}


def collect(settings: Settings, payload: dict, hour=9, minute=17, second=20) -> Path:
    """Simula uma coleta já gravada no bronze em DAY."""
    collected_at = datetime(DAY.year, DAY.month, DAY.day, hour, minute, second, tzinfo=TZ)
    return save_raw(payload, collected_at, "000", URL, settings)
