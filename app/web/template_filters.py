"""
Shared Jinja2 template filters and configured templates factory.
All routers should use `get_templates()` instead of creating their own Jinja2Templates.
"""
from datetime import datetime
from pathlib import Path
from fastapi.templating import Jinja2Templates

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

_templates: Jinja2Templates | None = None


def _format_date(value, fmt="%m/%d/%Y"):
    """Format a date value (string or datetime) for display. Returns 'N/A' for None."""
    if value is None:
        return "N/A"
    if isinstance(value, str):
        for parse_fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
            try:
                return datetime.strptime(value, parse_fmt).strftime(fmt)
            except (ValueError, TypeError):
                continue
        return value
    try:
        return value.strftime(fmt)
    except (AttributeError, TypeError):
        return str(value) if value else "N/A"


def _format_date_long(value):
    """Format date as 'March 15, 2025'."""
    return _format_date(value, fmt="%B %d, %Y")


def get_templates() -> Jinja2Templates:
    """Get shared Jinja2Templates instance with all custom filters registered."""
    global _templates
    if _templates is None:
        _templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
        _templates.env.filters["format_date"] = _format_date
        _templates.env.filters["format_date_long"] = _format_date_long
    return _templates
