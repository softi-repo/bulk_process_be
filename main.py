from datetime import datetime, timezone, timedelta
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware

from mangum import Mangum
from starlette.responses import JSONResponse

from dependencies.constants import Constants
from routes import api_router

app = FastAPI(
    title='Batch Processing Engine',
    description='Backend Engine for running end to end flow for API batches'
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=origins,
    allow_headers=origins,
)
app.include_router(api_router, prefix='/batch')

@app.middleware("http")
async def custom_header_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers.update(Constants.DEFAULT_RESPONSE_HEADERS)
    return response


@app.api_route("/{path_name:path}", methods=[
    "GET", "POST", "OPTIONS",
    "PATCH", "PUT", "DELETE"
], tags=["Fallback"])
async def catch_all(request: Request, path_name: str):

    available_routes = [
        {"path": route.path, "name": route.name}
        for route in app.routes if isinstance(route, APIRoute)
    ]

    return JSONResponse(
        {
            "base_url": str(request.base_url),
            "available_routes": available_routes,
            "request_method": request.method,
            "path_name": path_name,
            "message": "Unknown path received",
            "timestamp": (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).isoformat(),
        }
    )


# Lambda adapter to handle lambda events
# handler = Mangum(app)
