#!/usr/bin/env python3
"""
Tests for qqutils - Data display and export utilities (TDD approach)

Following TDD principles:
- Write tests first
- Use dependency injection (no mocks)
- Test with real objects (StringIO for file operations)
"""

import pytest
import sys
from io import StringIO
from typing import List, Dict
import pandas as pd

# Import functions to test (will fail until we implement them)
from qqutils import flatten_nested, to_dataframe, display_table, export_csv


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def simple_flat_data() -> List[Dict]:
    """Simple flat data structure"""
    return [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]


@pytest.fixture
def nested_data() -> List[Dict]:
    """Nested data structure (one level deep)"""
    return [
        {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
        {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
    ]


@pytest.fixture
def deeply_nested_data() -> List[Dict]:
    """Deeply nested data structure"""
    return [
        {
            "id": 1,
            "name": "Alice",
            "contact": {
                "email": "alice@example.com",
                "phone": {"mobile": "555-1234", "home": "555-5678"},
            },
        },
        {
            "id": 2,
            "name": "Bob",
            "contact": {
                "email": "bob@example.com",
                "phone": {"mobile": "555-9999", "home": "555-0000"},
            },
        },
    ]


@pytest.fixture
def data_with_lists() -> List[Dict]:
    """Data with list fields"""
    return [
        {"id": 1, "name": "Alice", "orders": [{"item": "laptop", "price": 1000}]},
        {"id": 2, "name": "Bob", "orders": [{"item": "mouse", "price": 20}]},
    ]


@pytest.fixture
def replication_like_data() -> List[Dict]:
    """Replication data structure (similar to actual use case)"""
    return [
        {
            "id": "rel-123",
            "source_root_path": "/data/source1",
            "target_root_path": "/data/target1",
            "state": "ESTABLISHED",
            "target_cluster_name": "cluster-dst",
            "recovery_point": "2025-10-28T10:30:00.000Z",
            "error_from_last_job": "",
        },
        {
            "id": "rel-456",
            "source_root_path": "/data/source2",
            "target_root_path": "/data/target2",
            "state": "REPLICATING",
            "target_cluster_name": "cluster-dst",
            "recovery_point": "2025-10-28T11:00:00.000Z",
            "error_from_last_job": "",
        },
    ]


# ============================================================================
# Tests for Pure Functions (Data Transformation)
# ============================================================================


class TestFlattenNested:
    """Tests for flatten_nested function (pure function, no I/O)"""

    def test_flatten_simple_nested(self, nested_data):
        """Should flatten one-level nested dictionaries"""
        result = flatten_nested(nested_data)

        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[0]["address.city"] == "NYC"
        assert result[0]["address.zip"] == "10001"
        assert result[1]["name"] == "Bob"
        assert result[1]["address.city"] == "LA"

    def test_flatten_deeply_nested(self, deeply_nested_data):
        """Should flatten deeply nested structures"""
        result = flatten_nested(deeply_nested_data)

        assert len(result) == 2
        assert result[0]["contact.email"] == "alice@example.com"
        assert result[0]["contact.phone.mobile"] == "555-1234"
        assert result[0]["contact.phone.home"] == "555-5678"

    def test_flatten_flat_data_unchanged(self, simple_flat_data):
        """Should return flat data unchanged"""
        result = flatten_nested(simple_flat_data)

        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30
        assert "address.city" not in result[0]

    def test_flatten_empty_data(self):
        """Should handle empty list"""
        result = flatten_nested([])
        assert result == []

    def test_flatten_with_record_path(self, data_with_lists):
        """Should handle record_path for nested lists"""
        result = flatten_nested(data_with_lists, record_path=["orders"])

        # Should expand list items to separate rows
        assert len(result) == 2
        assert result[0]["item"] == "laptop"
        assert result[0]["price"] == 1000

    def test_flatten_with_meta(self, data_with_lists):
        """Should preserve parent fields with meta parameter"""
        result = flatten_nested(
            data_with_lists, record_path=["orders"], meta=["id", "name"]
        )

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["item"] == "laptop"


class TestToDataframe:
    """Tests for to_dataframe function (pure function, no I/O)"""

    def test_to_dataframe_basic(self, simple_flat_data):
        """Should convert list of dicts to DataFrame"""
        df = to_dataframe(simple_flat_data)

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3, 3)  # 3 rows, 3 columns
        assert list(df.columns) == ["name", "age", "city"]
        assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_to_dataframe_with_column_selection(self, simple_flat_data):
        """Should select only specified columns"""
        df = to_dataframe(simple_flat_data, columns=["name", "city"])

        assert df.shape == (3, 2)
        assert list(df.columns) == ["name", "city"]
        assert "age" not in df.columns

    def test_to_dataframe_empty(self):
        """Should handle empty data"""
        df = to_dataframe([])

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_to_dataframe_nested(self, nested_data):
        """Should handle nested data (caller should flatten first)"""
        df = to_dataframe(nested_data)

        # Nested dict becomes object type
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 2)

    def test_to_dataframe_preserves_order(self, simple_flat_data):
        """Should preserve row order"""
        df = to_dataframe(simple_flat_data)

        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[1]["name"] == "Bob"
        assert df.iloc[2]["name"] == "Charlie"


# ============================================================================
# Tests for I/O Functions (Dependency Injection - use StringIO)
# ============================================================================


class TestDisplayTable:
    """Tests for display_table function (I/O via dependency injection)"""

    def test_display_table_basic(self, simple_flat_data):
        """Should output formatted table to provided output stream"""
        output = StringIO()
        display_table(simple_flat_data, output=output)

        result = output.getvalue()

        # Should contain data
        assert "Alice" in result
        assert "Bob" in result
        assert "Charlie" in result
        assert "30" in result
        assert "NYC" in result

    def test_display_table_with_columns(self, simple_flat_data):
        """Should display only selected columns"""
        output = StringIO()
        display_table(simple_flat_data, columns=["name", "age"], output=output)

        result = output.getvalue()

        assert "Alice" in result
        assert "30" in result
        # City should not be displayed
        assert "city" not in result.lower() or result.count("NYC") == 0

    def test_display_table_empty(self):
        """Should handle empty data gracefully"""
        output = StringIO()
        display_table([], output=output)

        result = output.getvalue()

        # Should indicate empty or produce minimal output
        assert result is not None

    def test_display_table_default_stdout(self, simple_flat_data, capsys):
        """Should default to sys.stdout if no output provided"""
        display_table(simple_flat_data)

        captured = capsys.readouterr()
        assert "Alice" in captured.out

    def test_display_table_nested_data(self, nested_data):
        """Should handle nested data (user should flatten first for best results)"""
        output = StringIO()
        display_table(nested_data, output=output)

        result = output.getvalue()
        assert "Alice" in result

    def test_display_table_replication_data(self, replication_like_data):
        """Should display replication-like data properly"""
        output = StringIO()
        display_table(replication_like_data, output=output)

        result = output.getvalue()

        assert "rel-123" in result
        assert "/data/source1" in result
        assert "ESTABLISHED" in result


class TestExportCsv:
    """Tests for export_csv function (I/O via dependency injection)"""

    def test_export_csv_basic(self, simple_flat_data):
        """Should write CSV to provided file handle"""
        file_handle = StringIO()
        export_csv(simple_flat_data, file_handle)

        file_handle.seek(0)
        content = file_handle.read()

        # Check header
        assert "name" in content
        assert "age" in content
        assert "city" in content

        # Check data
        assert "Alice" in content
        assert "30" in content
        assert "NYC" in content

    def test_export_csv_with_columns(self, simple_flat_data):
        """Should export only selected columns"""
        file_handle = StringIO()
        export_csv(simple_flat_data, file_handle, columns=["name", "city"])

        file_handle.seek(0)
        content = file_handle.read()

        assert "name" in content
        assert "city" in content
        # Age column should not be in output
        lines = content.strip().split("\n")
        header = lines[0]
        assert "age" not in header

    def test_export_csv_empty(self):
        """Should handle empty data"""
        file_handle = StringIO()
        export_csv([], file_handle)

        file_handle.seek(0)
        content = file_handle.read()

        # Should produce valid CSV (even if empty)
        assert content is not None

    def test_export_csv_preserves_order(self, simple_flat_data):
        """Should preserve row order in CSV"""
        file_handle = StringIO()
        export_csv(simple_flat_data, file_handle)

        file_handle.seek(0)
        lines = file_handle.readlines()

        # Skip header, check data rows
        assert "Alice" in lines[1]
        assert "Bob" in lines[2]
        assert "Charlie" in lines[3]

    def test_export_csv_handles_commas_in_data(self):
        """Should properly escape commas in data"""
        data = [{"name": "Smith, John", "age": 30}]
        file_handle = StringIO()
        export_csv(data, file_handle)

        file_handle.seek(0)
        content = file_handle.read()

        # CSV should handle comma in name properly (quoted)
        assert "Smith, John" in content or '"Smith, John"' in content

    def test_export_csv_replication_data(self, replication_like_data):
        """Should export replication-like data to CSV"""
        file_handle = StringIO()
        export_csv(replication_like_data, file_handle)

        file_handle.seek(0)
        content = file_handle.read()

        assert "source_root_path" in content
        assert "target_root_path" in content
        assert "/data/source1" in content
        assert "ESTABLISHED" in content


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests combining multiple functions"""

    def test_flatten_then_display(self, nested_data):
        """Should flatten nested data then display it"""
        flattened = flatten_nested(nested_data)
        output = StringIO()
        display_table(flattened, output=output)

        result = output.getvalue()

        assert "address.city" in result
        assert "NYC" in result

    def test_flatten_then_export(self, nested_data):
        """Should flatten nested data then export to CSV"""
        flattened = flatten_nested(nested_data)
        file_handle = StringIO()
        export_csv(flattened, file_handle)

        file_handle.seek(0)
        content = file_handle.read()

        assert "address.city" in content
        assert "NYC" in content

    def test_full_workflow_replication_data(self, replication_like_data):
        """Should handle full workflow: data -> display -> export"""
        # Display
        display_output = StringIO()
        display_table(replication_like_data, output=display_output)

        # Export
        csv_output = StringIO()
        export_csv(replication_like_data, csv_output)

        # Verify both outputs contain data
        assert "rel-123" in display_output.getvalue()
        assert "rel-123" in csv_output.getvalue()
