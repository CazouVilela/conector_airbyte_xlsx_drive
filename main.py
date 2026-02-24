#!/usr/bin/env python3
"""Entry point for the Airbyte Google Sheets XLSX source connector."""

from source_google_sheets_xlsx.source import run_cli

if __name__ == "__main__":
    run_cli()
