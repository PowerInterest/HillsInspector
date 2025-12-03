# Hillsborough County Property Data Acquisition Tool

## Overview
This tool is designed to acquire, analyze, and visualize property data from Hillsborough County, Florida. It focuses on identifying investment opportunities such as foreclosures, tax liens, and tax deeds.

## Architecture

The project follows a modular architecture:

*   **`src/`**: Contains the core logic for data acquisition, processing, and storage.
    *   **Data Sources**: Interactions with County Clerk, Tax Collector, and Property Appraiser websites.
    *   **AI Integration**: Uses the **Qwen3vl** model (via an OpenAI-compatible API) to navigate complex web pages, handle dynamic content, and bypass scraping challenges.
    *   **Storage**: Data is stored in a **DuckDB** database for efficient querying and analysis.
*   **`app/`**: A web application that serves as the user interface for viewing and interacting with the collected data.
*   **`main.py`**: The entry point for the data acquisition pipeline.

## Tech Stack
*   **Language**: Python 3.12+
*   **Package Manager**: `uv`
*   **Database**: DuckDB
*   **Web Automation**: Playwright
*   **AI Model**: Qwen3vl (OpenAI API format)
*   **CLI**: Typer

## Usage
(Instructions for running the scraper and starting the web app will be added here)
