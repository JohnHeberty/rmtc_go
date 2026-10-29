import uvicorn
from fastapi import FastAPI

from rmtc_go.core.settings import get_settings

from .routes.health import router as health_router

app = FastAPI(title="rmtc-go")
app.include_router(health_router)


def run() -> None:
    settings = get_settings()
    uvicorn.run(
        "rmtc_go.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
    )
