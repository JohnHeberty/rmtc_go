# Imagem única do projeto: roda a pipeline (locate-bus), a API (rmtc-api) e o servidor Prefect.
FROM ghcr.io/astral-sh/uv:0.13.2 AS uv

FROM python:3.15-slim AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependências primeiro: só reinstala quando o uv.lock muda
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.15-slim
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /data /prefect \
    && chown app:app /data /prefect
COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    TZ=America/Sao_Paulo \
    DATA_DIR=/data \
    PREFECT_HOME=/prefect \
    PREFECT_LOGGING_EXTRA_LOGGERS=rmtc_go \
    PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore

USER app
WORKDIR /app
VOLUME ["/data"]
CMD ["locate-bus", "serve"]
