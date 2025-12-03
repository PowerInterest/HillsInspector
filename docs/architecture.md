# System Architecture

## Directory Structure

```
.
├── app/                 # Web application for data visualization
│   └── ...
├── src/                 # Core business logic and data ingestion
│   ├── database.py      # DuckDB connection and schema management
│   ├── scrapers/        # Modules for different data sources
│   └── ai_client.py     # Qwen3vl API client wrapper
├── docs/                # Project documentation
├── main.py              # CLI entry point for running scrapers
├── pyproject.toml       # Dependencies and configuration
└── README.md            # Project overview
```

## Data Flow

1.  **Ingestion (`main.py`)**: The user triggers a search or scrape job via the CLI.
2.  **Acquisition (`src/scrapers/`)**:
    *   Playwright is used to navigate websites.
    *   **Qwen3vl** is invoked to interpret page content, identify elements, and make decisions (e.g., "click the download button", "solve this captcha", "parse this table").
3.  **Storage (`src/database.py`)**: Extracted data is normalized and stored in a local **DuckDB** database.
4.  **Presentation (`app/`)**: The web server connects to the DuckDB instance and displays properties in a user-friendly interface.

## AI Integration
The project utilizes the Qwen3vl model to handle the complexity of modern web pages. Instead of brittle CSS/XPath selectors, the system sends screenshots or DOM snapshots to the AI, which returns structured data or navigation actions. This makes the scrapers more resilient to website changes.
