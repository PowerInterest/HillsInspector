# Web Application Code Review: Findings & Recommendations

This document outlines the findings from a detailed code review of the `app/web/` application structure, focusing on logic bugs, silent failures, and logging practices.

## 1. Silent Failures & Defeated Global Error Handlers

The most critical issue across the web application is the extensive use of "silent failures" in the data access layer.

**The Anti-Pattern:**
In almost all database-accessing functions within `app/web/pg_web.py`, `app/web/pg_database.py`, and `app/web/routers/properties.py`, exceptions are caught and swallowed, returning an empty default value (e.g., `[]`, `None`, `{}`).

```python
# Example from pg_web.py:get_upcoming_auctions
except Exception as e:
    logger.warning(f"get_upcoming_auctions failed: {e}")
    return []
```

**The Impact:**
1. **Masked 500 Errors:** The application has a very robust global exception handler in `app/web/main.py` (`@app.exception_handler(Exception)`) designed to generate UUID error IDs, log full tracebacks, and return formatted 500 pages or HTMX error fragments. Because the data access layer catches and swallows the exceptions, this **global handler is never reached**. A failing database query results in a 200 OK response with "no data" rather than a 500 error.
2. **Hidden Bugs:** During development and production, if a table is dropped, a column is renamed, or there is a SQL syntax error, the UI simply appears empty. There is no visible failure, making bugs extremely hard to detect and trace.

## 2. Poor Logging: Swallowed Tracebacks

When exceptions are caught, the logging approach used strips crucial debugging information.

**The Anti-Pattern:**
```python
except Exception as e:
    logger.warning(f"Function failed: {e}")
```

**The Impact:**
By interpolating the exception as a string (`{e}`), only the immediate error message is textually logged. The stack trace context (file line numbers, call stack) is lost.

**Recommendation:**
When handling exceptions where a traceback is necessary, use `logger.exception("...")` or `logger.error("...", exc_info=True)`. However, the better solution (outlined below) is to simply not catch the exception at all, letting `main.py` log the traceback.

## 3. HTMX State Masking

Because backend queries swallow errors and return `None`, the HTMX partial views exhibit confusing behavior during a failure.

**The Anti-Pattern:**
In `app/web/routers/properties.py`, when a tab is clicked via HTMX (e.g., `/property/{folio}/permits`), the router calls `_pg_property_detail(folio)`. If the database errors out, it returns `None`. The router does this:
```python
    prop = _pg_property_detail(folio)
    if not prop:
        return HTMLResponse("<p>Property not found</p>")
```

**The Impact:**
If an underlying SQL query for the property detail fails, clicking *any* tab on the property page simply renders the text "Property not found" inside the tab container, with a `200 OK` status code. The frontend has no way to know an error occurred and no error styling is applied.

## 4. Vestigial Template Logic

There are remnants of older implementation logic that no longer serve a purpose and clutter the codebase.

**The Finding:**
In `app/web/routers/properties.py`, the `property_liens` route hardcodes the `liens` variable to an empty list:
```python
    return templates.TemplateResponse(
        "partials/lien_table.html",
        {"request": request, "liens": [], "encumbrances": encumbrances, ...},
    )
```
However, `app/web/templates/partials/lien_table.html` contains extensive Jinja logic attempting to iterate over `liens`:
```html
{% if not encumbrances %}
{% for lien in liens %}
...
{% endfor %}
{% endif %}
```
Since `liens` is hardcoded to `[]`, this block evaluates but never executes. This is leftover boilerplate from before the SQLite to Postgres (ORI) data migration.

## Proposed Solutions

To improve the robustness and maintainability of the web application, the following refactoring steps are recommended:

1. **Remove Bare Exception Swallowing:**
   In `pg_web.py`, `pg_database.py`, and `routers/*.py`, remove the `try... except Exception... return []` blocks surrounding SQL execution. Allow `sqlalchemy.exc.SQLAlchemyError` and other exceptions to bubble up.
2. **Leverage the Global Handler:**
   Rely on `main.py`'s `unhandled_exception_handler`. This will automatically capture the full traceback, assign a UUID to the error, log it securely, and return the appropriate HTTP 500 / JSON / HTMX error fragment to the client.
3. **Graceful Degradation (Where Appropriate):**
   If a specific query is truly optional and should not crash a page (e.g., fetching a dashboard statistic), catch the specific exception (e.g., `OperationalError`), use `logger.exception` to log the traceback, and then provide the fallback. However, this should be the exception, not the rule.
4. **Clean up Templates:**
   Remove the vestigial `liens` iteration and related checks from `lien_table.html`. The source of truth for liens is now the `encumbrances` array.
