from dataclasses import dataclass
from pathlib import Path

from rmtc_go.core.logging import get_logger
from rmtc_go.core.settings import Settings
from rmtc_go.core.storage import BRONZE, read_json

from .config import DATASET
from .extract import content_hash, version_files

logger = get_logger(__name__)


@dataclass(frozen=True)
class DedupResult:
    files: int  # versões verificadas
    removed: list[str]  # caminhos relativos ao bronze/linha_kml


def version_hash(path: Path) -> str | None:
    """Hash recalculado do KML da versão; None se o arquivo não abrir."""
    try:
        return content_hash(read_json(path)["payload"])
    except ValueError, OSError, EOFError, KeyError, TypeError, AttributeError:
        return None


def find_duplicates(versions: list[Path]) -> list[Path]:
    """Versões com o KML igual ao da versão anterior mantida do mesmo traçado.

    Só compara com a anterior: uma rota que volta ao traçado antigo (A, B, A) é mudança real.
    Arquivos que não abrem não são tocados (o load os manda para a quarentena).
    """
    duplicates: list[Path] = []
    previous: str | None = None
    for path in versions:
        current = version_hash(path)
        if current is None:
            continue
        if current == previous:
            duplicates.append(path)
        else:
            previous = current
    return duplicates


def remove_duplicates(settings: Settings) -> DedupResult:
    """Apaga do bronze as versões de cada traçado iguais à anterior."""
    root = settings.data_dir / BRONZE / DATASET
    n_files = 0
    removed: list[str] = []
    for directory in sorted(root.glob("linha=*/sentido=*/arquivo=*")):
        versions = version_files(directory)
        n_files += len(versions)
        for path in find_duplicates(versions):
            removed.append(path.relative_to(root).as_posix())
            path.unlink()

    if removed:
        logger.info("dedup %s: %d versões duplicadas removidas: %s", DATASET, len(removed), removed)
    return DedupResult(files=n_files, removed=removed)
