"""
FastAPI application factory.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from polymarket.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Polymarket Data API",
        description=(
            "Real-time query API for Polymarket on-chain trading data. "
            "Supports pre-built endpoints and custom SQL queries via DuckDB."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    @app.get("/")
    async def root():
        return {
            "service": "Polymarket Data API",
            "docs": "/docs",
            "status": "/api/status"
        }

    return app
