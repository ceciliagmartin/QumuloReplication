#!/usr/bin/env python3
"""
Qumulo Utilities - Data display and export helpers

Simple, reusable functions for displaying and exporting data.
Uses pandas for consistent formatting across display and CSV export.

Design principles:
- Pure functions for data transformation (no I/O side effects)
- Dependency injection for I/O operations (testable without mocks)
- Data structure agnostic (works with any dict/list data)

Usage:
    from qqutils import display_table, export_csv, flatten_nested

    # Display data as table
    display_table(data)

    # Export to CSV
    with open('output.csv', 'w') as f:
        export_csv(data, f)

    # Flatten nested structures
    flat_data = flatten_nested(nested_data)
"""

import sys
from typing import List, Dict, Any, Optional, TextIO
import pandas as pd


def flatten_nested(
    data: List[Dict[str, Any]],
    record_path: Optional[List[str]] = None,
    meta: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten nested dictionaries/lists into flat structure.

    Uses pandas json_normalize for consistent flattening.
    Nested keys become dot-separated: {"a": {"b": 1}} -> {"a.b": 1}

    Args:
        data: List of dictionaries (possibly nested)
        record_path: Path to nested list to expand (e.g., ["orders"])
        meta: Parent fields to preserve when using record_path

    Returns:
        List of flattened dictionaries

    Examples:
        >>> data = [{"user": {"name": "Alice", "age": 30}}]
        >>> flatten_nested(data)
        [{"user.name": "Alice", "user.age": 30}]

        >>> data = [{"id": 1, "orders": [{"item": "laptop"}]}]
        >>> flatten_nested(data, record_path=["orders"], meta=["id"])
        [{"id": 1, "item": "laptop"}]
    """
    if not data:
        return []

    # Use pandas json_normalize to flatten
    if record_path:
        df = pd.json_normalize(data, record_path=record_path, meta=meta)
    else:
        df = pd.json_normalize(data)

    # Convert back to list of dicts
    return df.to_dict(orient="records")


def to_dataframe(
    data: List[Dict[str, Any]], columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Convert list of dictionaries to pandas DataFrame.

    Pure function - no I/O side effects.

    Args:
        data: List of dictionaries
        columns: Optional list of columns to include (default: all)

    Returns:
        pandas DataFrame

    Examples:
        >>> data = [{"name": "Alice", "age": 30}]
        >>> df = to_dataframe(data)
        >>> df.shape
        (1, 2)
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Select columns if specified
    if columns:
        # Only select columns that exist in the DataFrame
        available_columns = [col for col in columns if col in df.columns]
        df = df[available_columns]

    return df


def display_table(
    data: List[Dict[str, Any]],
    columns: Optional[List[str]] = None,
    output: TextIO = None,
) -> None:
    """
    Display data as formatted table to output stream.

    Uses pandas DataFrame with left-aligned columns for better text readability.
    Dependency injection for output allows testing with StringIO.

    Args:
        data: List of dictionaries to display
        columns: Optional list of columns to display (default: all)
        output: Output stream (default: sys.stdout)

    Examples:
        >>> data = [{"name": "Alice", "age": 30}]
        >>> display_table(data)  # Prints to stdout

        >>> from io import StringIO
        >>> output = StringIO()
        >>> display_table(data, output=output)  # Capture output
    """
    if output is None:
        output = sys.stdout

    if not data:
        output.write("No data to display.\n")
        return

    df = to_dataframe(data, columns=columns)

    # Convert all columns to string for consistent formatting
    df = df.astype(str)

    # Get column widths (max of column name or data width)
    col_widths = {}
    for col in df.columns:
        col_widths[col] = max(len(str(col)), df[col].str.len().max())

    # Format header (left-aligned)
    header = " ".join(f"{col:<{col_widths[col]}}" for col in df.columns)
    output.write(header)
    output.write("\n")

    # Format rows (left-aligned)
    for _, row in df.iterrows():
        row_str = " ".join(f"{str(row[col]):<{col_widths[col]}}" for col in df.columns)
        output.write(row_str)
        output.write("\n")


def export_csv(
    data: List[Dict[str, Any]],
    file_handle: TextIO,
    columns: Optional[List[str]] = None,
) -> None:
    """
    Export data to CSV format via file handle.

    Uses pandas to_csv() for proper CSV formatting (handles escaping, etc).
    Dependency injection for file handle allows testing with StringIO.

    Args:
        data: List of dictionaries to export
        file_handle: Open file handle (text mode) to write to
        columns: Optional list of columns to export (default: all)

    Examples:
        >>> data = [{"name": "Alice", "age": 30}]
        >>> with open('output.csv', 'w') as f:
        ...     export_csv(data, f)

        >>> from io import StringIO
        >>> output = StringIO()
        >>> export_csv(data, output)  # Capture CSV for testing
    """
    if not data:
        # Write empty CSV with no headers
        file_handle.write("")
        return

    df = to_dataframe(data, columns=columns)

    # Use pandas to_csv for proper formatting
    df.to_csv(file_handle, index=False)
