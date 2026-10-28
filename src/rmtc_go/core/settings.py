from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """Configuração lida de variáveis de ambiente (e do .env na raiz do projeto)."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = "https://www.rmtcgoiania.com.br/"
    prefix_url: str = "index.php"

    data_dir: Path = Field(default=PROJECT_ROOT / "database")
    timezone: str = "America/Sao_Paulo"

    # Agendamento da pipeline locate_bus (cron no fuso de `timezone`)
    locate_bus_cron: str = "* * * * *"
    # Traçados (KML): uma vez por dia, depois da falha de dados da RMTC das 04h
    linha_kml_cron: str = "30 5 * * *"

    http_timeout: float = 30.0
    http_retries: int = 3

    api_host: str = "0.0.0.0"
    api_port: int = 3001
    api_reload: bool = False

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


@lru_cache
def get_settings() -> Settings:
    return Settings()
