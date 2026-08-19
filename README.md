# FRED Economic Data Pipeline

An automated data pipeline that extracts economic time series from the Federal Reserve Economic Data (FRED) API, loads them incrementally into DuckDB, and runs on a daily schedule using GitHub Actions.

## Overview

This project demonstrates a complete data engineering workflow: API extraction, incremental loading, local warehousing, and CI/CD scheduled automation, built with modern open source tooling instead of a paid orchestration platform.

## Data Source

Data is pulled from the [FRED API](https://fred.stlouisfed.org/docs/api/fred/), the Federal Reserve Bank of St. Louis's public economic data service. The pipeline currently tracks five series relevant to manufacturing, banking, and retail analytics:

| Series ID | Description |
|---|---|
| UNRATE | Unemployment rate |
| CPIAUCSL | Consumer Price Index (inflation) |
| FEDFUNDS | Federal funds rate |
| INDPRO | Industrial production index |
| RSXFS | Retail sales |

## Tech Stack

- **Python** for pipeline logic
- **dlt (data load tool)** for extraction, schema handling, and incremental state management
- **DuckDB** as the destination warehouse
- **GitHub Actions** for daily scheduled runs
- **requests** for API calls, with retry and exponential backoff on failures

## How It Works

1. Each economic series is loaded through its own dlt resource, with its own independent incremental cursor tracking the last date seen.
2. On every run, the pipeline only requests observations newer than the last recorded date per series, avoiding redundant API calls and reloading.
3. All series write into a single merged table (`fred_observations`) in DuckDB, keyed on `series_id` and `date`.
4. GitHub Actions runs the pipeline daily at 06:00 UTC. It can also be triggered manually from the Actions tab.

## Project Structure

```
.
├── fred_pipeline.py              # Main pipeline script
├── requirements.txt              # Python dependencies
├── requirements_github_action.txt # Dependencies used by the GitHub Actions runner
├── .dlt/
│   └── config.toml                # Non-secret dlt configuration
├── .github/
│   └── workflows/
│       └── run_fred_pipeline_workflow.yml   # Scheduled GitHub Actions workflow
└── README.md
```

Note: `.dlt/secrets.toml` holds the FRED API key locally and is excluded from version control via `.gitignore`. In GitHub Actions, the key is provided through a repository secret instead.

## Setup

1. Clone the repo and create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Get a free FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html

3. Add it locally in `.dlt/secrets.toml`:
   ```toml
   [sources.fred_pipeline]
   api_key = "your_fred_api_key_here"
   ```

4. Run the pipeline:
   ```bash
   python fred_pipeline.py
   ```

## Automated Runs

The pipeline runs automatically every day via GitHub Actions. To enable this in your own fork:

1. Add a repository secret named `SOURCES__FRED_PIPELINE__API_KEY` with your FRED API key value, under **Settings > Secrets and variables > Actions**.
2. The workflow will run on schedule, or can be triggered manually from the **Actions** tab.

## Why This Project

Built as a portfolio piece to demonstrate practical data engineering skills: working with REST APIs, designing correct incremental loading logic (including catching and fixing a shared-cursor bug across multiple series), warehousing with DuckDB, and automating recurring pipelines with GitHub Actions rather than manual runs.

## Author

Abdul Moiz Waheed
Data Analyst and Analytics Engineer
[GitHub](https://github.com/MoizWaheed80)
