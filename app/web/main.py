"""Active FastAPI entrypoint for the HillsInspector web dashboard.

Architectural purpose:
- expose the PG-first dashboard and API routers used for operational review,
  property inspection, and history pages;
- provide the supported local launcher for the web app when developers run
  `uv run python -m app.web.main`;
- optionally open an `ngrok` HTTP tunnel from the active web entrypoint so
  remote access uses the same application process as local access.

How this fits into the broader system:
- `Controller.py` remains the pipeline entrypoint;
- this module is the dashboard entrypoint only;
- the tunnel lifecycle is kept here rather than in archived/legacy launchers so
  the current PG-first app has one authoritative runtime path.
"""

import argparse
import os
import socket
import traceback
import uuid
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.web.routers import dashboard, properties, api, review, history, database_view
from app.web.exceptions import DatabaseLockedError, DatabaseUnavailableError


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

# Setup templates (shared instance with custom filters)
from app.web.template_filters import get_templates  # noqa: E402
templates = get_templates()

# Include routers
app.include_router(dashboard.router)
app.include_router(properties.router, prefix="/property")
app.include_router(api.router, prefix="/api")
app.include_router(review.router, prefix="/review")
app.include_router(history.router)
app.include_router(database_view.router)


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
    from app.web.pg_web import check_database_health
    db_status = check_database_health()
    return {
        "status": "ok" if db_status["available"] else "degraded",
        "service": "HillsInspector",
        "database": db_status
    }


if __name__ == "__main__":
    def _build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Run the HillsInspector web dashboard")
        parser.add_argument(
            "--host",
            default=os.getenv("WEB_HOST", "0.0.0.0"),
            help="Host interface to bind (default: WEB_HOST or 0.0.0.0)",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=int(os.getenv("WEB_PORT", "8080")),
            help="Port to bind (default: WEB_PORT or 8080)",
        )
        parser.add_argument(
            "--reload",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="Enable auto-reload while developing (default: on)",
        )
        parser.add_argument(
            "--ngrok",
            action="store_true",
            help="Start an ngrok HTTP tunnel for remote access",
        )
        return parser


    def _get_local_ip() -> str:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect(("10.255.255.255", 1))
            return sock.getsockname()[0]
        except OSError as exc:
            logger.debug("Falling back to localhost IP detection: {}", exc)
            return "127.0.0.1"
        finally:
            sock.close()


    def _start_ngrok_tunnel(port: int) -> str | None:
        try:
            from pyngrok import ngrok
        except ImportError:
            logger.error("pyngrok is not installed; starting local server without ngrok")
            return None

        auth_token = os.getenv("NGROK_AUTHTOKEN", "").strip()
        if auth_token:
            ngrok.set_auth_token(auth_token)

        logger.info("Starting ngrok tunnel for port {}", port)
        tunnel = ngrok.connect(str(port), "http")
        public_url = tunnel.public_url
        logger.success("ngrok tunnel established: {}", public_url)
        print()
        print("=" * 72)
        print(f"  PUBLIC URL: {public_url}")
        print("=" * 72)
        print()
        return public_url


    def _stop_ngrok_tunnel(public_url: str | None) -> None:
        if not public_url:
            return

        from pyngrok import ngrok

        logger.info("Closing ngrok tunnel: {}", public_url)
        ngrok.disconnect(public_url)


    def main() -> None:
        args = _build_parser().parse_args()

        logger.info("Starting HillsInspector Web on {}:{}", args.host, args.port)
        logger.info("Local Access: http://localhost:{}", args.port)
        if args.host == "0.0.0.0":
            ip_addr = _get_local_ip()
            if ip_addr != "127.0.0.1":
                logger.info("Network Access: http://{}:{}", ip_addr, args.port)

        public_url: str | None = None
        if args.ngrok:
            try:
                public_url = _start_ngrok_tunnel(args.port)
            except Exception as exc:
                logger.error("Failed to start ngrok: {}", exc)
                logger.info(
                    "Configure ngrok with `ngrok config add-authtoken <token>` "
                    "or set NGROK_AUTHTOKEN"
                )

        try:
            uvicorn.run(
                "app.web.main:app",
                host=args.host,
                port=args.port,
                reload=args.reload,
            )
        finally:
            _stop_ngrok_tunnel(public_url)


    main()
