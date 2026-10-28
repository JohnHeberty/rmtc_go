from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session(retries: int = 3, headers: dict[str, str] | None = None) -> Session:
    """Session HTTP com retry e backoff exponencial para erros transitórios (429 e 5xx)."""
    retry = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    session = Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    if headers:
        session.headers.update(headers)
    return session
