"""
Standalone schema-mapping tool to standardize equipment GeoPackages using
the Electric device schema from Substation Fields.xlsx.

Usage (example):
python mapping_tool.py \
  --gpkg CURRENT_TRANSFORMER.gpkg \
  --layer CURRENT_TRANSFORMER \
  --schema Substation Fields.xlsx \
  --sheet "Electric device" \
  --equipment "Current Transformer" \
  --output CURRENT_TRANSFORMER_standardized.gpkg
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

MAX_GPKG_NAME_LENGTH = 254
_NUM_REGEX = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


# ---------------------------- String helpers -----------------------------
def strip_unicode_spaces(text: str) -> str:
    if not isinstance(text, str):
        return text
    return "".join(ch for ch in text if ch not in ("\ufeff", "\u200b", "\u200c", "\u200d", "\xa0"))


def _clean_column_name(name: Any) -> str:
    text = "" if name is None else str(name)
    text = strip_unicode_spaces(text)
    text = " ".join(text.split())
    return text.strip()


def normalize_for_compare(name: Any) -> str:
    if name is None:
        return ""
    text = strip_unicode_spaces(str(name)).lower()
    text = " ".join(text.split())
    remove_chars = " -_,./()\\"
    return text.translate(str.maketrans("", "", remove_chars)).strip()


# ---------------------------- Schema loaders -----------------------------
def list_schema_equipments(schema_path: Path, sheet_name: str, device_col: int = 0) -> list[str]:
    df = pd.read_excel(schema_path, sheet_name=sheet_name, dtype=str, header=None)
    devices = df.iloc[:, device_col].ffill().dropna().map(_clean_column_name).map(str.strip)
    devices = [d for d in devices if d]
    devices = [d for d in devices if normalize_for_compare(d) not in ("device", "equipment")]
    return sorted(set(devices))


def load_schema_fields(
    schema_path: Path,
    sheet_name: str,
    equipment_name: str,
    device_col: int = 0,
    field_col: int = 1,
    type_col: int = 2,
) -> tuple[list[str], dict[str, str]]:
    schema_raw = pd.read_excel(schema_path, sheet_name=sheet_name, dtype=str, header=None)
    schema_raw.iloc[:, device_col] = schema_raw.iloc[:, device_col].ffill()
    mask = schema_raw.iloc[:, device_col].fillna("").map(normalize_for_compare) == normalize_for_compare(equipment_name)
    schema_df = schema_raw.loc[mask].copy()

    # Ensure columns exist
    while schema_df.shape[1] <= max(field_col, type_col):
        schema_df[schema_df.shape[1]] = None
    schema_df.columns = [f"col_{i}" for i in range(schema_df.shape[1])]
    field_series = schema_df.iloc[:, field_col]
    type_series = schema_df.iloc[:, type_col]

    schema_df = pd.DataFrame({"field": field_series, "type": type_series})
    schema_df["field"] = schema_df["field"].fillna("").map(_clean_column_name)
    schema_df["type"] = schema_df["type"].fillna("").map(str)
    schema_df = schema_df[
        schema_df["field"].map(
            lambda x: x
            and normalize_for_compare(x)
            not in (
                "field",
                "fieldname",
                "datatype",
                "datatype",
                "data type",
            )
        )
    ]
    fields = schema_df["field"].tolist()
    type_map = dict(zip(schema_df["field"], schema_df["type"]))
    return fields, type_map


# ---------------------------- Mapping helpers ----------------------------
def fuzzy_map_columns(source_cols: list[str], target_fields: list[str], threshold: float = 0.6) -> dict[str, str]:
    norm_target = {normalize_for_compare(t): t for t in target_fields}
    result: dict[str, str] = {}
    for src in source_cols:
        norm_src = normalize_for_compare(src)
        best = None
        best_score = threshold
        for nt, tname in norm_target.items():
            ratio = 0
            # simple ratio; difflib not imported to keep lightweight
            min_len = max(len(norm_src), len(nt))
            if min_len:
                overlap = len(set(norm_src) & set(nt)) / min_len
                ratio = overlap
            if norm_src and nt and (norm_src in nt or nt in norm_src):
                ratio = max(ratio, 0.9)
            if ratio > best_score or (abs(ratio - best_score) < 1e-6 and best and len(tname) < len(best)):
                best = tname
                best_score = ratio
        if best and best not in result:
            result[best] = src
    return result


def _extract_first_number(value: Any) -> float | None:
    if pd.isna(value):
        return None
    text = str(value).replace("−", "-")
    m = _NUM_REGEX.search(text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def coerce_series_to_type(series: pd.Series, type_str: str) -> pd.Series:
    t = normalize_for_compare(type_str or "")
    if not isinstance(series, pd.Series):
        return series
    if "int" in t:
        coerced = series.map(_extract_first_number)
        return pd.Series(coerced, dtype="Int64")
    if any(tok in t for tok in ("double", "float", "decimal", "real")):
        coerced = series.map(_extract_first_number)
        return pd.Series(coerced, dtype="float64")
    if "bool" in t:
        return series.astype("boolean")
    return series.astype("string")


# ---------------------------- GPKG helpers -------------------------------
def ensure_valid_gpkg_dtypes(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64tz_dtype(series):
        series = series.dt.tz_localize(None)
    elif pd.api.types.is_timedelta64_dtype(series):
        series = series.astype(str)
    if pd.api.types.is_object_dtype(series) or any(
        isinstance(v, (list, dict, set, tuple)) for v in series.dropna().head(5)
    ):
        series = series.apply(lambda v: _clean_column_name(v) if isinstance(v, str) else (str(v) if v is not None else None))
    if pd.api.types.is_numeric_dtype(series):
        series = series.astype("float64" if pd.api.types.is_float_dtype(series) else series.dtype)
    return series


def sanitize_gdf_for_gpkg(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    geometry_name = out.geometry.name
    new_cols = []
    used = {}
    for col in out.columns:
        if col == geometry_name:
            new_cols.append(col)
            continue
        c = _clean_column_name(col)
        if len(c) > MAX_GPKG_NAME_LENGTH:
            c = c[:MAX_GPKG_NAME_LENGTH]
        base = c
        counter = 1
        while c in used:
            suffix = f"_{counter}"
            limit = MAX_GPKG_NAME_LENGTH - len(suffix)
            c = (base[:limit] if len(base) > limit else base) + suffix
            counter += 1
        used[c] = True
        new_cols.append(c)
    out.columns = new_cols

    for col in out.columns:
        if col == geometry_name:
            continue
        series = ensure_valid_gpkg_dtypes(out[col])
        mask = pd.isna(series)
        if mask.any():
            series = series.astype(object)
            series[mask] = None
        out[col] = series
    return out


# ---------------------------- Core mapping -------------------------------
def map_layer_to_schema(
    gdf: gpd.GeoDataFrame,
    schema_fields: list[str],
    type_map: dict[str, str],
    keep_unmatched: bool = True,
    threshold: float = 0.6,
) -> gpd.GeoDataFrame:
    suggested = fuzzy_map_columns(list(gdf.columns), schema_fields, threshold=threshold)
    out_cols: dict[str, pd.Series] = {}
    for f in schema_fields:
        src = suggested.get(f)
        if src and src in gdf.columns:
            out_cols[f] = gdf[src]
        else:
            out_cols[f] = pd.NA
    if keep_unmatched:
        for col in gdf.columns:
            if col not in suggested.values() and col != gdf.geometry.name:
                out_cols[f"orig_{col}"] = gdf[col]

    for f in schema_fields:
        out_cols[f] = coerce_series_to_type(out_cols[f], type_map.get(f, ""))

    out_gdf = gpd.GeoDataFrame(out_cols, geometry=gdf.geometry, crs=gdf.crs)
    out_gdf = sanitize_gdf_for_gpkg(out_gdf)
    return out_gdf


# ---------------------------- CLI ----------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Standardize an equipment GPKG using Electric device schema.")
    p.add_argument("--gpkg", required=True, help="Input equipment GeoPackage")
    p.add_argument("--layer", required=False, help="Layer name (optional if single-layer GPKG)")
    p.add_argument("--schema", required=True, help="Schema workbook path (e.g., Substation Fields.xlsx)")
    p.add_argument("--sheet", default="Electric device", help="Schema sheet name")
    p.add_argument("--equipment", required=True, help="Equipment/device name in schema (e.g., Current Transformer)")
    p.add_argument("--output", required=False, help="Output GPKG path")
    p.add_argument("--threshold", type=float, default=0.6, help="Fuzzy match threshold (0-1)")
    p.add_argument("--keep-unmatched", action="store_true", help="Keep unmatched original columns as orig_*")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    gpkg_path = Path(args.gpkg)
    layer = args.layer
    schema_path = Path(args.schema)
    out_path = Path(args.output) if args.output else gpkg_path.with_name(gpkg_path.stem + "_standardized.gpkg")

    gdf = gpd.read_file(gpkg_path, layer=layer) if layer else gpd.read_file(gpkg_path)
    schema_fields, type_map = load_schema_fields(schema_path, args.sheet, args.equipment)
    mapped = map_layer_to_schema(gdf, schema_fields, type_map, keep_unmatched=args.keep_unmatched, threshold=args.threshold)

    layer_name = (layer or gpkg_path.stem).replace(" ", "_")[:MAX_GPKG_NAME_LENGTH]
    mapped.to_file(out_path, driver="GPKG", layer=layer_name)
    print(f"Written standardized GPKG to {out_path}")


if __name__ == "__main__":
    main()
