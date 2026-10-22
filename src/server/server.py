from fastapi import FastAPI
from src.routes.health import health

app = FastAPI()

app.include_router(health)
