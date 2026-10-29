from datetime import datetime

import pytest

from rmtc_go.pipelines.linha_kml import extract as extract_module
from tests.conftest import DAY, TZ


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
