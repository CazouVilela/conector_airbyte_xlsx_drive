"""Unit tests for the Google Sheets XLSX source connector."""

from __future__ import annotations

import io
import json
from datetime import date, datetime, time
from unittest.mock import MagicMock, patch

import openpyxl
import pytest

from source_google_sheets_xlsx.source import (
    GoogleSheetsStream,
    SourceGoogleSheetsXlsx,
    _extract_sheet_data_xlsx,
    build_schema_from_sample,
    deduplicate_headers,
    filter_empty_rows,
    infer_json_format,
    infer_json_type,
    sanitize_column_name,
    serialize_value,
    strip_trailing_none_columns,
    MIME_XLSX,
    MIME_GOOGLE_SHEETS,
)


# ---------------------------------------------------------------------------
# sanitize_column_name
# ---------------------------------------------------------------------------

class TestSanitizeColumnName:
    def test_basic(self):
        assert sanitize_column_name("Hello World") == "hello_world"

    def test_accents(self):
        assert sanitize_column_name("Descrição") == "descricao"
        assert sanitize_column_name("Préço Médio") == "preco_medio"

    def test_special_chars(self):
        assert sanitize_column_name("valor (R$)") == "valor_r"

    def test_empty(self):
        assert sanitize_column_name("") == "unnamed"
        assert sanitize_column_name(None) == "unnamed"

    def test_numbers(self):
        assert sanitize_column_name("col123") == "col123"

    def test_multiple_underscores(self):
        assert sanitize_column_name("a  --  b") == "a_b"


# ---------------------------------------------------------------------------
# deduplicate_headers
# ---------------------------------------------------------------------------

class TestDeduplicateHeaders:
    def test_no_duplicates(self):
        assert deduplicate_headers(["a", "b", "c"]) == ["a", "b", "c"]

    def test_duplicates(self):
        assert deduplicate_headers(["col", "col", "col"]) == ["col", "col_1", "col_2"]

    def test_mixed(self):
        assert deduplicate_headers(["x", "y", "x", "z", "y"]) == ["x", "y", "x_1", "z", "y_1"]


# ---------------------------------------------------------------------------
# serialize_value
# ---------------------------------------------------------------------------

class TestSerializeValue:
    def test_none(self):
        assert serialize_value(None) is None

    def test_datetime(self):
        val = datetime(2024, 1, 15, 10, 30, 0)
        assert serialize_value(val) == "2024-01-15T10:30:00"

    def test_date(self):
        val = date(2024, 1, 15)
        assert serialize_value(val) == "2024-01-15"

    def test_time(self):
        val = time(10, 30)
        assert serialize_value(val) == "10:30:00"

    def test_numeric(self):
        assert serialize_value(42) == 42
        assert serialize_value(3.14) == 3.14

    def test_string(self):
        assert serialize_value("hello") == "hello"

    def test_bool(self):
        assert serialize_value(True) is True

    def test_other_type(self):
        assert serialize_value([1, 2]) == "[1, 2]"


# ---------------------------------------------------------------------------
# infer_json_type / infer_json_format
# ---------------------------------------------------------------------------

class TestInferTypes:
    def test_none(self):
        assert infer_json_type(None) == "string"

    def test_bool(self):
        assert infer_json_type(True) == "boolean"

    def test_int(self):
        assert infer_json_type(42) == "integer"

    def test_float(self):
        assert infer_json_type(3.14) == "number"

    def test_datetime(self):
        assert infer_json_type(datetime(2024, 1, 1)) == "string"
        assert infer_json_format(datetime(2024, 1, 1)) == "date-time"

    def test_date(self):
        assert infer_json_format(date(2024, 1, 1)) == "date"

    def test_time(self):
        assert infer_json_format(time(10, 0)) == "time"

    def test_string_no_format(self):
        assert infer_json_format("hello") is None


# ---------------------------------------------------------------------------
# strip_trailing_none_columns
# ---------------------------------------------------------------------------

class TestStripTrailingNoneColumns:
    def test_strips_trailing(self):
        headers = ["a", "b", "", ""]
        rows = [[1, 2, None, None], [3, 4, None, None]]
        h, r = strip_trailing_none_columns(headers, rows)
        assert h == ["a", "b"]
        assert r == [[1, 2], [3, 4]]

    def test_no_trailing(self):
        headers = ["a", "b"]
        rows = [[1, 2]]
        h, r = strip_trailing_none_columns(headers, rows)
        assert h == ["a", "b"]

    def test_empty(self):
        h, r = strip_trailing_none_columns([], [])
        assert h == []


# ---------------------------------------------------------------------------
# filter_empty_rows
# ---------------------------------------------------------------------------

class TestFilterEmptyRows:
    def test_filters(self):
        rows = [[1, 2], [None, None], [3, None]]
        result = filter_empty_rows(rows)
        assert result == [[1, 2], [3, None]]

    def test_all_empty(self):
        assert filter_empty_rows([[None], [None, None]]) == []


# ---------------------------------------------------------------------------
# build_schema_from_sample
# ---------------------------------------------------------------------------

class TestBuildSchema:
    def test_basic(self):
        headers = ["name", "age"]
        rows = [["Alice", 30], ["Bob", 25]]
        schema = build_schema_from_sample(headers, rows)
        assert "properties" in schema
        assert schema["properties"]["name"]["type"] == ["string", "null"]
        assert schema["properties"]["age"]["type"] == ["integer", "null"]

    def test_mixed_types(self):
        headers = ["val"]
        rows = [[1], ["text"], [3.14]]
        schema = build_schema_from_sample(headers, rows)
        assert schema["properties"]["val"]["type"] == ["string", "null"]

    def test_empty_column(self):
        headers = ["empty"]
        rows = [[None], [None]]
        schema = build_schema_from_sample(headers, rows)
        assert schema["properties"]["empty"]["type"] == ["string", "null"]


# ---------------------------------------------------------------------------
# GoogleSheetsStream
# ---------------------------------------------------------------------------

class TestGoogleSheetsStream:
    def test_name(self):
        stream = GoogleSheetsStream("vendas", ["col1"], [[1]])
        assert stream.name == "vendas"

    def test_schema(self):
        stream = GoogleSheetsStream("test", ["a", "b"], [[1, "x"], [2, "y"]])
        schema = stream.get_json_schema()
        assert "a" in schema["properties"]
        assert "b" in schema["properties"]

    def test_read_records(self):
        stream = GoogleSheetsStream(
            "test",
            ["name", "value"],
            [["Alice", 100], ["Bob", 200]],
        )
        records = list(stream.read_records())
        assert len(records) == 2
        assert records[0] == {"name": "Alice", "value": 100}
        assert records[1] == {"name": "Bob", "value": 200}

    def test_ragged_rows(self):
        stream = GoogleSheetsStream(
            "test",
            ["a", "b", "c"],
            [["x"]],
        )
        records = list(stream.read_records())
        assert records[0] == {"a": "x", "b": None, "c": None}

    def test_datetime_serialization(self):
        dt = datetime(2024, 6, 15, 14, 30)
        stream = GoogleSheetsStream("test", ["ts"], [[dt]])
        records = list(stream.read_records())
        assert records[0]["ts"] == "2024-06-15T14:30:00"


# ---------------------------------------------------------------------------
# XLSX extraction (integration with openpyxl, mocked Drive)
# ---------------------------------------------------------------------------

class TestExtractXlsx:
    def test_extract_sheets(self, sample_xlsx_bytes):
        """Test XLSX extraction with mocked Drive download."""
        mock_drive = MagicMock()

        with patch(
            "source_google_sheets_xlsx.source._download_xlsx"
        ) as mock_dl:
            mock_dl.return_value = io.BytesIO(sample_xlsx_bytes)
            sheets = _extract_sheet_data_xlsx(mock_drive, "fake_id", names_conversion=True)

        assert len(sheets) == 2

        vendas = sheets[0]
        assert vendas["name"] == "Vendas"
        assert "data" in vendas["headers"]
        assert "produto" in vendas["headers"]
        assert "valor" in vendas["headers"]
        # Trailing None cols should be stripped (5 cols -> 3)
        assert len(vendas["headers"]) == 3
        # Empty row should be filtered (4 data rows -> 3)
        assert len(vendas["rows"]) == 3

        clientes = sheets[1]
        assert clientes["name"] == "Clientes"
        assert len(clientes["rows"]) == 2

    def test_empty_xlsx(self, empty_xlsx_bytes):
        """Empty sheets should be skipped."""
        with patch(
            "source_google_sheets_xlsx.source._download_xlsx"
        ) as mock_dl:
            mock_dl.return_value = io.BytesIO(empty_xlsx_bytes)
            sheets = _extract_sheet_data_xlsx(MagicMock(), "fake_id", names_conversion=True)

        assert len(sheets) == 0

    def test_duplicate_headers(self, xlsx_with_duplicates_bytes):
        """Duplicate headers should get _1, _2 suffixes."""
        with patch(
            "source_google_sheets_xlsx.source._download_xlsx"
        ) as mock_dl:
            mock_dl.return_value = io.BytesIO(xlsx_with_duplicates_bytes)
            sheets = _extract_sheet_data_xlsx(MagicMock(), "fake_id", names_conversion=True)

        assert sheets[0]["headers"] == ["col", "col_1", "col_2"]


# ---------------------------------------------------------------------------
# SourceGoogleSheetsXlsx - spec
# ---------------------------------------------------------------------------

class TestSourceSpec:
    def test_spec_loads(self):
        source = SourceGoogleSheetsXlsx()
        spec = source.spec()
        assert "connectionSpecification" in spec
        props = spec["connectionSpecification"]["properties"]
        assert "credentials_json" in props
        assert "spreadsheet_id" in props
        assert "names_conversion" in props


# ---------------------------------------------------------------------------
# SourceGoogleSheetsXlsx - discover
# ---------------------------------------------------------------------------

class TestSourceDiscover:
    def test_discover_xlsx(self, sample_xlsx_bytes):
        source = SourceGoogleSheetsXlsx()
        config = {
            "credentials_json": '{"type":"service_account","project_id":"test"}',
            "spreadsheet_id": "test_id",
            "names_conversion": True,
        }

        with patch("source_google_sheets_xlsx.source._build_google_services") as mock_svc, \
             patch("source_google_sheets_xlsx.source._detect_file_type") as mock_detect, \
             patch("source_google_sheets_xlsx.source._download_xlsx") as mock_dl:

            mock_detect.return_value = MIME_XLSX
            mock_dl.return_value = io.BytesIO(sample_xlsx_bytes)
            mock_svc.return_value = (MagicMock(), MagicMock())

            catalog = source.discover(config)

        assert "streams" in catalog
        assert len(catalog["streams"]) == 2
        stream_names = [s["name"] for s in catalog["streams"]]
        assert "vendas" in stream_names
        assert "clientes" in stream_names
