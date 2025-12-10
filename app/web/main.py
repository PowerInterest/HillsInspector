"""
HillsInspector Web Interface
FastAPI + Jinja2 + HTMX
"""
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pathlib import Path
from contextlib import asynccontextmanager
from loguru import logger

from app.web.routers import dashboard, properties, api, review


from src.utils.logging_config import setup_default_logging

# Configure loguru
setup_default_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("HillsInspector Web starting up...")
    yield
    logger.info("HillsInspector Web shutting down...")


# Create FastAPI app
app = FastAPI(
    title="HillsInspector",
    description="Hillsborough County Property Auction Analysis",
    version="0.1.0",
    lifespan=lifespan
)

# Paths
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Setup templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Include routers
app.include_router(dashboard.router)
app.include_router(properties.router, prefix="/property")
app.include_router(api.router, prefix="/api")
app.include_router(review.router, prefix="/review")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "HillsInspector"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.web.main:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )
