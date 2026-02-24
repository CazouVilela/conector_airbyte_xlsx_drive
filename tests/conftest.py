"""Fixtures with mocked Google services for testing."""

from __future__ import annotations

import io
from datetime import datetime
from unittest.mock import MagicMock, patch

import openpyxl
import pytest

from source_google_sheets_xlsx.source import MIME_GOOGLE_SHEETS, MIME_XLSX


def _make_xlsx_bytes(sheets_data: dict[str, list[list]]) -> bytes:
    """Create an in-memory XLSX file from {sheet_name: [[row], ...]} data."""
    wb = openpyxl.Workbook()
    # Remove default sheet
    wb.remove(wb.active)
    for name, rows in sheets_data.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.read()


@pytest.fixture
def sample_xlsx_bytes():
    """XLSX with two sheets, some empty rows and trailing None columns."""
    return _make_xlsx_bytes({
        "Vendas": [
            ["Data", "Produto", "Valor", None, None],
            [datetime(2024, 1, 15), "Widget A", 100.50, None, None],
            [datetime(2024, 1, 16), "Widget B", 200.75, None, None],
            [None, None, None, None, None],  # empty row
            [datetime(2024, 1, 17), "Widget C", 50.00, None, None],
        ],
        "Clientes": [
            ["Nome", "Email"],
            ["Alice", "alice@example.com"],
            ["Bob", "bob@example.com"],
        ],
    })


@pytest.fixture
def empty_xlsx_bytes():
    """XLSX with one empty sheet."""
    return _make_xlsx_bytes({"Vazia": []})


@pytest.fixture
def xlsx_with_duplicates_bytes():
    """XLSX with duplicate column headers."""
    return _make_xlsx_bytes({
        "Dup": [
            ["col", "col", "col"],
            ["a", "b", "c"],
            ["d", "e", "f"],
        ],
    })


@pytest.fixture
def mock_drive_service_xlsx(sample_xlsx_bytes):
    """Mock Drive service that returns an XLSX file."""
    service = MagicMock()

    # files().get() for MIME type detection
    files_get = MagicMock()
    files_get.execute.return_value = {"mimeType": MIME_XLSX}
    service.files.return_value.get.return_value = files_get

    # files().get_media() for download
    media_request = MagicMock()
    media_request.execute.return_value = sample_xlsx_bytes
    # Simulate MediaIoBaseDownload by making next_chunk write bytes
    media_request.uri = "https://www.googleapis.com/download/drive/v3/files/test"
    media_request.http = MagicMock()

    def side_effect_get_media(**kwargs):
        return media_request

    service.files.return_value.get_media = side_effect_get_media

    return service


@pytest.fixture
def mock_sheets_service_native():
    """Mock Sheets service for native Google Sheets."""
    service = MagicMock()

    # spreadsheets().get() returns metadata
    meta = {
        "sheets": [
            {"properties": {"title": "Vendas"}},
            {"properties": {"title": "Clientes"}},
        ]
    }
    service.spreadsheets.return_value.get.return_value.execute.return_value = meta

    # spreadsheets().values().get() returns data per sheet
    def values_get(**kwargs):
        sheet_range = kwargs.get("range", "")
        mock_result = MagicMock()
        if sheet_range == "Vendas":
            mock_result.execute.return_value = {
                "values": [
                    ["Data", "Produto", "Valor"],
                    ["2024-01-15", "Widget A", "100.50"],
                    ["2024-01-16", "Widget B", "200.75"],
                ]
            }
        elif sheet_range == "Clientes":
            mock_result.execute.return_value = {
                "values": [
                    ["Nome", "Email"],
                    ["Alice", "alice@example.com"],
                    ["Bob", "bob@example.com"],
                ]
            }
        else:
            mock_result.execute.return_value = {"values": []}
        return mock_result

    service.spreadsheets.return_value.values.return_value.get = values_get

    return service


@pytest.fixture
def base_config():
    """Minimal connector config."""
    return {
        "credentials_json": '{"type": "service_account", "project_id": "test"}',
        "spreadsheet_id": "test_spreadsheet_id",
        "names_conversion": True,
    }
