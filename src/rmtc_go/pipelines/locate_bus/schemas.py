from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel, Field, field_validator

from rmtc_go.core.settings import get_settings

# ---------- Bronze: envelope do dado bruto ----------


class BronzeMetadata(BaseModel):
    dataset: str
    source_url: str
    linha: str
    collected_at: AwareDatetime
    # Hash do payload: coletas com o mesmo valor trouxeram exatamente os mesmos dados.
    # Opcional porque coletas antigas não têm (o load recalcula).
    payload_sha256: str | None = None


class BronzeRecord(BaseModel):
    """Arquivo do bronze: metadados da coleta + payload exatamente como a API devolveu."""

    metadata: BronzeMetadata
    payload: dict[str, Any]


# ---------- Contrato da API (validado no load, não no extract) ----------


class LocateBusResponse(BaseModel):
    status: str
    # Itens validados um a um no load, para que um ônibus inválido não descarte a coleta
    onibus: list[dict[str, Any]] = Field(default_factory=list)


# DestinoCurto e os KML: a chave é obrigatória, mas o valor pode vir null. Visto em
# 28/10 das 04:01 às 05:00: a RMTC mandou ônibus em operação (posição válida) sem destino
# ou sem KML, e todos voltaram completos às 05:00. Quando preenchidos, a validação vale.


class InfoDestino(BaseModel):
    DestinoCurto: str | None = Field(min_length=3, max_length=50)


class InfoLinha(BaseModel):
    LinhaNumero: str = Field(min_length=1, max_length=4)
    LinhaKMLIda: str | None
    LinhaKMLVolta: str | None

    @field_validator("LinhaKMLIda", "LinhaKMLVolta")
    @classmethod
    def mesmo_dominio_da_api(cls, value: str | None) -> str | None:
        if value is None:
            return value
        expected = urlparse(get_settings().base_url).netloc
        if urlparse(value).netloc != expected:
            raise ValueError(
                f"Domínio do link da rota ({value}) diferente da API ({expected}), algo mudou."
            )
        return value


class BusRaw(BaseModel):
    Numero: int = Field(ge=1, le=99999)
    Latitude: float = Field(ge=-23, le=-12)
    Longitude: float = Field(ge=-54, le=-45)
    Acessivel: int = Field(ge=0, le=1)
    Situacao: str = Field(min_length=4, max_length=12)
    Linha: InfoLinha
    Destino: InfoDestino


# ---------- Silver ----------


class BusSilver(BaseModel):
    Numero: int
    Latitude: float
    Longitude: float
    Acessivel: int
    Situacao: str
    LinhaNumero: str
    LinhaKMLIda: str | None
    LinhaKMLVolta: str | None
    DestinoCurto: str | None
    ColetadoEm: datetime
    ColetaSha256: str

    @classmethod
    def from_raw(cls, bus: BusRaw, collected_at: datetime, payload_sha256: str) -> BusSilver:
        return cls(
            Numero=bus.Numero,
            Latitude=bus.Latitude,
            Longitude=bus.Longitude,
            Acessivel=bus.Acessivel,
            Situacao=bus.Situacao,
            LinhaNumero=bus.Linha.LinhaNumero,
            LinhaKMLIda=bus.Linha.LinhaKMLIda,
            LinhaKMLVolta=bus.Linha.LinhaKMLVolta,
            DestinoCurto=bus.Destino.DestinoCurto,
            ColetadoEm=collected_at,
            ColetaSha256=payload_sha256,
        )


def silver_schema(timezone: str) -> pa.Schema:
    """Schema fixo do parquet: todas as partições com os mesmos tipos, mesmo quando vazias."""
    return pa.schema(
        [
            ("Numero", pa.int64()),
            ("Latitude", pa.float64()),
            ("Longitude", pa.float64()),
            ("Acessivel", pa.int64()),
            ("Situacao", pa.string()),
            ("LinhaNumero", pa.string()),
            ("LinhaKMLIda", pa.string()),
            ("LinhaKMLVolta", pa.string()),
            ("DestinoCurto", pa.string()),
            ("ColetadoEm", pa.timestamp("us", tz=timezone)),
            ("ColetaSha256", pa.string()),
        ]
    )
