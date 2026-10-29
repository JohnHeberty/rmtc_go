import logging

ROOT_LOGGER = "rmtc_go"

# Sem isso o pacote herda WARNING do root nas execuções do Prefect (onde setup_logging não roda)
logging.getLogger(ROOT_LOGGER).setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """Logger do projeto. Na UI do Prefect, exige PREFECT_LOGGING_EXTRA_LOGGERS=rmtc_go."""
    return logging.getLogger(name if name.startswith(ROOT_LOGGER) else f"{ROOT_LOGGER}.{name}")


def setup_logging(level: int = logging.INFO) -> None:
    """Configura saída no console para execuções via CLI."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    # O cliente do Prefect loga cada requisição HTTP em INFO
    logging.getLogger("httpx").setLevel(logging.WARNING)
