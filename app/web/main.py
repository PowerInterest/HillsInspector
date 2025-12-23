"""
HillsInspector Web Interface
FastAPI + Jinja2 + HTMX
"""
import traceback
import uuid
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from pathlib import Path
from contextlib import asynccontextmanager
from loguru import logger

from app.web.routers import dashboard, properties, api, review, history
from app.web.database import DatabaseLockedError, DatabaseUnavailableError


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
app.include_router(history.router)


# =============================================================================
# Error Handlers
# =============================================================================

def _generate_error_id() -> str:
    """Generate a short error ID for tracking."""
    return str(uuid.uuid4())[:8].upper()


def _is_htmx_request(request: Request) -> bool:
    """Check if request is from HTMX."""
    return request.headers.get("HX-Request") == "true"


def _is_api_request(request: Request) -> bool:
    """Check if request is for API endpoint."""
    return request.url.path.startswith("/api/")


def _error_html(
    request: Request,
    status_code: int,
    title: str,
    message: str,
    details: str | None = None,
    error_id: str | None = None
) -> HTMLResponse:
    """Generate error HTML response."""
    # For HTMX requests, return a small error fragment
    if _is_htmx_request(request):
        html = f"""
        <div class="alert alert-danger" role="alert">
            <h5>{title}</h5>
            <p>{message}</p>
            {f'<small class="text-muted">Error ID: {error_id}</small>' if error_id else ''}
        </div>
        """
        return HTMLResponse(content=html, status_code=status_code)

    # Full error page
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{status_code} - {title}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{ background: #f8f9fa; min-height: 100vh; display: flex; align-items: center; }}
            .error-container {{ max-width: 600px; }}
            .error-code {{ font-size: 6rem; font-weight: bold; color: #dc3545; }}
            .error-details {{ background: #fff3cd; border-radius: 8px; padding: 1rem; margin-top: 1rem; }}
            .error-details pre {{ margin: 0; white-space: pre-wrap; font-size: 0.8rem; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-container mx-auto text-center">
                <div class="error-code">{status_code}</div>
                <h1 class="mb-3">{title}</h1>
                <p class="lead text-muted">{message}</p>
                {f'<p class="text-muted"><small>Error ID: <code>{error_id}</code></small></p>' if error_id else ''}
                {f'<div class="error-details text-start"><strong>Details:</strong><pre>{details}</pre></div>' if details else ''}
                <div class="mt-4">
                    <a href="/" class="btn btn-primary">Back to Dashboard</a>
                    <button onclick="location.reload()" class="btn btn-outline-secondary ms-2">Retry</button>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html, status_code=status_code)


@app.exception_handler(DatabaseLockedError)
async def database_locked_handler(request: Request, exc: DatabaseLockedError):
    """Handle database locked errors gracefully."""
    error_id = _generate_error_id()
    logger.warning(f"Database locked [ID: {error_id}]: {exc} - {request.url}")

    if _is_api_request(request):
        return JSONResponse(
            status_code=503,
            content={
                "error": "database_locked",
                "message": "Database is temporarily unavailable. The pipeline may be running.",
                "error_id": error_id,
                "retry_after": 30
            },
            headers={"Retry-After": "30"}
        )

    return _error_html(
        request,
        status_code=503,
        title="Database Busy",
        message="The database is currently locked by another process (likely the data pipeline). Please try again in a few moments.",
        error_id=error_id
    )


@app.exception_handler(DatabaseUnavailableError)
async def database_unavailable_handler(request: Request, exc: DatabaseUnavailableError):
    """Handle database unavailable errors."""
    error_id = _generate_error_id()
    logger.error(f"Database unavailable [ID: {error_id}]: {exc} - {request.url}")

    if _is_api_request(request):
        return JSONResponse(
            status_code=503,
            content={
                "error": "database_unavailable",
                "message": str(exc),
                "error_id": error_id
            }
        )

    return _error_html(
        request,
        status_code=503,
        title="Database Unavailable",
        message=str(exc),
        error_id=error_id
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with detailed output."""
    error_id = _generate_error_id()

    # Log 4xx and 5xx errors
    if exc.status_code >= 400:
        log_fn = logger.warning if exc.status_code < 500 else logger.error
        log_fn(f"HTTP {exc.status_code} [ID: {error_id}]: {exc.detail} - {request.method} {request.url}")

    if _is_api_request(request):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": "http_error",
                "status_code": exc.status_code,
                "message": exc.detail,
                "error_id": error_id,
                "path": str(request.url.path)
            }
        )

    # Map status codes to user-friendly titles
    titles = {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        422: "Validation Error",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
    }

    return _error_html(
        request,
        status_code=exc.status_code,
        title=titles.get(exc.status_code, "Error"),
        message=exc.detail or "An error occurred",
        error_id=error_id
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions with detailed logging."""
    error_id = _generate_error_id()

    # Get full traceback for logging
    tb = traceback.format_exc()
    logger.error(
        f"Unhandled exception [ID: {error_id}]\n"
        f"Request: {request.method} {request.url}\n"
        f"Exception: {type(exc).__name__}: {exc}\n"
        f"Traceback:\n{tb}"
    )

    if _is_api_request(request):
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": f"An unexpected error occurred: {type(exc).__name__}",
                "error_id": error_id,
                "details": str(exc),
                "path": str(request.url.path)
            }
        )

    # For web requests, show error details (useful for debugging)
    # In production, you might want to hide details
    details = f"{type(exc).__name__}: {exc}\n\n{tb}"

    return _error_html(
        request,
        status_code=500,
        title="Internal Server Error",
        message="An unexpected error occurred while processing your request.",
        details=details,
        error_id=error_id
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    from app.web.database import check_database_health
    db_status = check_database_health()
    return {
        "status": "ok" if db_status["available"] else "degraded",
        "service": "HillsInspector",
        "database": db_status
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.web.main:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )
