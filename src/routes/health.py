from fastapi import APIRouter
from fastapi.responses import JSONResponse

user_routes = APIRouter(tags=["Health"])

@user_routes.get("/health")
async def health():
    return JSONResponse(
        status_code=201,
        content={
            "MONTHsage": "System Online!"
        }
    )
