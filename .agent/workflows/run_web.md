---
description: Run the HillsInspector Web Interface
---

# Run Web Interface

This workflow explains how to launch the web dashboard for exploring properties and generating title reports.

## Steps

1.  **Start the Server**:
    ```bash
    uv run uvicorn app.web.main:app --host 0.0.0.0 --port 8080 --reload
    ```

2.  **Access Dashboard**:
    Open your browser and navigate to:
    [http://localhost:8080](http://localhost:8080)

3.  **Generate Title Report**:
    -   Click on a property to view details.
    -   Append `/title-report` to the URL (or click the "Title Report" button if added to the UI).
    -   Example: `http://localhost:8080/property/{folio}/title-report`

## Features

-   **Dashboard**: View upcoming auctions (next 60 days).
-   **Filtering**: Filter by Foreclosure/Tax Deed.
-   **Title Report**: Professional, printable PDF-style report.
-   **Chain of Title**: View ownership history directly on the property page.
