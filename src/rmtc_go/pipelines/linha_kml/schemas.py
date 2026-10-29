import pyarrow as pa
from pydantic import AwareDatetime, BaseModel

# ---------- Bronze ----------


class KmlMetadata(BaseModel):
    dataset: str
    linha: str
    sentido: str
    source_url: str  # sem o ?rev=, que muda a cada hora sem mudar o arquivo
    collected_at: AwareDatetime
    payload_sha256: str
    # cabeçalhos HTTP do servidor: Last-Modified é quando o arquivo mudou de fato
    last_modified: AwareDatetime | None = None
    etag: str | None = None


class KmlBronzeRecord(BaseModel):
    """Versão do bronze: metadados + o KML exatamente como a RMTC devolveu."""

    metadata: KmlMetadata
    payload: str


class FetchEntry(BaseModel):
    """Uma tentativa de download. Fica registrada mesmo sem mudança ou com falha: é o que prova
    que um traçado foi conferido (ou não) num momento."""

    linha: str
    sentido: str
    arquivo: str
    url: str
    fetched_at: AwareDatetime
    status: str  # "ok" | "erro"
    erro: str | None = None
    sha256: str | None = None
    salvo: str | None = None  # versão gravada nesta tentativa (relativo a bronze/linha_kml)
    last_modified: AwareDatetime | None = None  # ausente nos registros até 28/10
    etag: str | None = None


class FetchLog(BaseModel):
    """Registro de uma execução do extract: bronze/linha_kml/_fetch/<timestamp>.json.gz."""

    dataset: str
    started_at: AwareDatetime
    window: list[str]  # dias do silver de posições usados para descobrir os KML
    entries: list[FetchEntry]


# ---------- Silver ----------


def observacao_schema(timezone: str) -> pa.Schema:
    """Uma linha por verificação de um KML (com ou sem mudança, com ou sem falha)."""
    return pa.schema(
        [
            ("Url", pa.string()),
            ("Arquivo", pa.string()),
            ("LinhaNumero", pa.string()),
            ("Sentido", pa.string()),
            ("ObservadoEm", pa.timestamp("us", tz=timezone)),
            ("Status", pa.string()),
            ("Erro", pa.string()),
            ("Sha256", pa.string()),  # do KML bruto
            ("HashGeometria", pa.string()),  # da geometria normalizada (WKT)
            ("ModificadoEm", pa.timestamp("us", tz=timezone)),  # Last-Modified do servidor
            ("Origem", pa.string()),  # "fetch" (registro da execução) | "versao" (só o arquivo)
        ]
    )


def versao_schema(timezone: str) -> pa.Schema:
    """Uma linha por versão do traçado de cada arquivo KML.

    A troca para esta versão aconteceu em algum momento em (TrocaApos, PrimeiraObs]: o
    extract só vê o KML quando baixa. Quando o Last-Modified do servidor cai nessa janela,
    ele dá o horário exato (Precisao "exata"); senão ValidoDesde = PrimeiraObs ("janela", ou
    "desde_1a_obs" na 1ª versão, em que não se sabe desde quando valia).
    """
    ts = pa.timestamp("us", tz=timezone)
    return pa.schema(
        [
            ("Url", pa.string()),
            ("Arquivo", pa.string()),
            ("LinhaNumero", pa.string()),
            ("Sentido", pa.string()),
            ("Versao", pa.int64()),  # 1, 2, 3... por arquivo
            ("Sha256", pa.string()),
            ("HashGeometria", pa.string()),
            ("Nome", pa.string()),
            ("Geometria", pa.string()),  # WKT (lon lat): LINESTRING ou MULTILINESTRING
            ("Pontos", pa.int64()),
            ("TrocaApos", ts),  # última vez que a versão anterior foi vista (null: 1ª versão)
            ("PrimeiraObs", ts),
            ("UltimaObs", ts),  # última confirmação desta versão
            ("SubstituidaEm", ts),  # PrimeiraObs da próxima versão (null: mais recente)
            ("Observacoes", pa.int64()),
            ("ModificadoEm", ts),  # menor Last-Modified visto nesta versão
            ("ValidoDesde", ts),
            ("ValidoAte", ts),  # ValidoDesde da próxima versão (null: mais recente)
            ("Precisao", pa.string()),  # "exata" | "janela" | "desde_1a_obs"
        ]
    )


def uso_schema(timezone: str) -> pa.Schema:
    """Períodos em que uma linha/sentido usou um KML, a partir das posições (precisão de coleta).

    Um período termina quando a linha troca de arquivo ou fica sem posição por mais de
    USO_GAP_MINUTES. Períodos que passam da meia-noite são cortados no fim do dia.
    """
    ts = pa.timestamp("us", tz=timezone)
    return pa.schema(
        [
            ("LinhaNumero", pa.string()),
            ("Sentido", pa.string()),
            ("Url", pa.string()),
            ("Arquivo", pa.string()),
            ("UsoDesde", ts),
            ("UsoAte", ts),
            ("Coletas", pa.int64()),  # coletas de posição com a linha usando este arquivo
        ]
    )
