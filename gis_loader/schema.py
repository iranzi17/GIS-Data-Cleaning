import re
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ELECTRIC_DEVICE_EQUIPMENT
from .data_sources import cache_key_from_path, excel_key_from_file, get_excel_file
from .text import (
    clean_column_name,
    ensure_unique_columns,
    normalize_for_compare,
    strip_unicode_spaces,
)

_SHEET_HEADER_CACHE: dict[tuple[str, str], list[str]] = {}
_REFERENCE_SHEET_CACHE: dict[tuple[str, str], pd.DataFrame] = {}
_NUM_REGEX = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def apply_global_forward_fill(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize empty-like cells and forward-fill workbook tables."""
    if df.empty:
        return df

    def _normalize_empty(val: Any):
        if isinstance(val, str):
            cleaned = strip_unicode_spaces(val).strip()
            if cleaned == "" or cleaned.lower() in {"nan", "none", "null"}:
                return pd.NA
            return val
        if pd.isna(val):
            return pd.NA
        return val

    normalized = df.applymap(_normalize_empty)
    return normalized.ffill()


def clean_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop fully empty rows and re-apply forward-fill normalization."""
    if df.empty:
        return df
    mask = df.apply(lambda col: col.map(lambda value: (pd.isna(value) if not isinstance(value, str) else not value.strip())))
    cleaned = df.loc[~mask.all(axis=1)].copy()
    cleaned.columns = df.columns
    cleaned = apply_global_forward_fill(cleaned)
    return cleaned


def detect_header_row(raw_df: pd.DataFrame) -> int:
    """Pick the most likely header row based on substation/header density."""
    best_row = 0
    best_score = -1
    for idx, row in raw_df.head(10).iterrows():
        cleaned_cells = [clean_column_name(cell) for cell in row]
        substation_hits = sum("substation" in normalize_for_compare(cell) for cell in cleaned_cells if isinstance(cell, str))
        non_empty = sum(bool(str(cell).strip()) for cell in cleaned_cells)
        score = substation_hits * 10 + min(non_empty, 5)
        if score > best_score:
            best_score = score
            best_row = idx
    return best_row


def get_sheet_header(excel_file: pd.ExcelFile, sheet: str) -> list[str] | None:
    """Return cleaned header for a sheet using a small cached preview read."""
    key = (excel_key_from_file(excel_file), sheet)
    if key in _SHEET_HEADER_CACHE:
        return _SHEET_HEADER_CACHE[key]
    try:
        raw_df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str, header=None, nrows=15)
        header_row = detect_header_row(raw_df)
        header = ensure_unique_columns([clean_column_name(cell) for cell in raw_df.iloc[header_row]])
        _SHEET_HEADER_CACHE[key] = header
        return header
    except Exception:
        return None


def load_schema_fields(
    schema_path: Path,
    sheet_name: str,
    equipment_name: str | None,
    header_row: int | None = None,
    device_col: int = 0,
    field_col: int | None = None,
    type_col: int | None = None,
) -> tuple[list[str], dict[str, str]]:
    """Load field names and types for a specific equipment/device from a schema sheet."""
    schema_raw = pd.read_excel(schema_path, sheet_name=sheet_name, dtype=str, header=None)

    def _detect_header_and_cols(df: pd.DataFrame) -> tuple[int, int | None, int | None]:
        header_row_det = 0
        field_col_det = None
        type_col_det = None
        for idx, row in df.head(5).iterrows():
            for col_idx, val in row.items():
                norm = normalize_for_compare(val)
                if not norm:
                    continue
                if "type" in norm or "tpe" in norm:
                    type_col_det = col_idx
                if "field" in norm and norm not in ("device", "equipment"):
                    if field_col_det is None or "fieldname" in norm:
                        field_col_det = col_idx
            if type_col_det is not None and field_col_det is not None:
                header_row_det = idx
                break
        return header_row_det, field_col_det, type_col_det

    header_det, field_det, type_det = _detect_header_and_cols(schema_raw)

    if sheet_name.lower().strip() == "hydro pp":
        header_row = 0 if header_row is None else header_row
        field_col = 1 if field_col is None else field_col
        type_col = (schema_raw.shape[1] - 1) if type_col is None else type_col
    else:
        header_row = header_row if header_row is not None else header_det
        field_col = field_col if field_col is not None else (field_det if field_det is not None else 1)
        type_col = type_col if type_col is not None else (type_det if type_det is not None else schema_raw.shape[1] - 1)

    schema_df = schema_raw.copy()
    schema_df.iloc[:, device_col] = schema_df.iloc[:, device_col].ffill()

    if header_row is not None and len(schema_df) > header_row:
        schema_df = schema_df.iloc[header_row + 1 :]

    if equipment_name is not None:
        target_norm = normalize_for_compare(equipment_name)
        mask = schema_df.iloc[:, device_col].fillna("").map(normalize_for_compare) == target_norm
        schema_df = schema_df.loc[mask].copy()

    while schema_df.shape[1] <= max(field_col, type_col):
        schema_df[schema_df.shape[1]] = None

    schema_df.columns = [f"col_{idx}" for idx in range(schema_df.shape[1])]
    field_series = schema_df.iloc[:, field_col]
    type_series = schema_df.iloc[:, type_col]

    schema_df = pd.DataFrame({"field": field_series, "type": type_series})
    schema_df["field"] = schema_df["field"].fillna("").map(clean_column_name)
    schema_df["type"] = schema_df["type"].fillna("").map(str)
    schema_df = schema_df[schema_df["field"] != ""]
    schema_df = schema_df[schema_df["field"].map(lambda value: normalize_for_compare(value) not in ("field", "fieldname"))]
    fields = schema_df["field"].tolist()
    type_map = dict(zip(schema_df["field"], schema_df["type"]))
    for field in list(type_map.keys()):
        if normalize_for_compare(field) == normalize_for_compare("Manufacturer"):
            type_map[field] = "Text"
    return fields, type_map


def load_reference_sheet(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    """Load and clean a sheet from the reference workbook using the main loader rules."""
    cache_key = (cache_key_from_path(workbook_path), sheet_name)
    cached = _REFERENCE_SHEET_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    excel_file = get_excel_file(workbook_path)
    raw_df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str, header=None)
    header_row = detect_header_row(raw_df)
    header = [clean_column_name(cell) for cell in raw_df.iloc[header_row]]
    header = ensure_unique_columns(header)
    df = raw_df.iloc[header_row + 1 :].copy()
    df.columns = header
    df.reset_index(drop=True, inplace=True)
    df = apply_global_forward_fill(df)
    df = clean_empty_rows(df)
    _REFERENCE_SHEET_CACHE[cache_key] = df
    return df.copy()


def list_schema_equipments(schema_path: Path, sheet_name: str, device_col: int = 0) -> list[str]:
    """List unique equipment/device names from a schema sheet."""
    if normalize_for_compare(sheet_name) == normalize_for_compare("Electric device"):
        return ELECTRIC_DEVICE_EQUIPMENT
    schema_raw = pd.read_excel(schema_path, sheet_name=sheet_name, dtype=str, header=None)
    devices = schema_raw.iloc[:, device_col].ffill().dropna().map(clean_column_name).map(str.strip)
    devices = [device for device in devices if device]
    devices = [device for device in devices if normalize_for_compare(device) not in ("device", "equipment")]
    return sorted(set(devices))


def extract_first_number(value: Any) -> float | None:
    """Extract the first numeric token from a value."""
    if pd.isna(value):
        return None
    text = str(value).replace("âˆ’", "-")
    text = text.replace("\u2212", "-")
    match = _NUM_REGEX.search(text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def coerce_series_to_type(series: pd.Series, type_str: str) -> pd.Series:
    """Coerce a series to the target schema type using lenient parsing."""
    target_type = normalize_for_compare(type_str or "")
    if not isinstance(series, pd.Series):
        return series
    if any(token in target_type for token in ("date", "datetime", "timestamp")):
        return pd.to_datetime(series, errors="coerce")
    if "short" in target_type and "int" in target_type:
        coerced = series.map(extract_first_number)
        return pd.Series(coerced, dtype="Int16")
    if "long" in target_type and "int" in target_type:
        coerced = series.map(extract_first_number)
        return pd.Series(coerced, dtype="Int32")
    if "short" in target_type and "int" not in target_type:
        coerced = series.map(extract_first_number)
        return pd.Series(coerced, dtype="Int16")
    if any(token in target_type for token in ("int", "integer", "bigint", "smallint")):
        coerced = series.map(extract_first_number)
        return pd.Series(coerced, dtype="Int64")
    if any(token in target_type for token in ("double", "float", "decimal", "real", "number")):
        coerced = series.map(extract_first_number)
        return pd.Series(coerced, dtype="float64")
    if "bool" in target_type:
        try:
            return series.astype("boolean")
        except Exception:
            return series.map(
                lambda value: str(value).strip().lower() in {"true", "1", "yes"} if pd.notna(value) else pd.NA
            ).astype("boolean")
    return series.astype("string")
