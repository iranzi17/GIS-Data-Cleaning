from pathlib import Path

import geopandas as gpd
import pandas as pd

from .config import MAX_GPKG_NAME_LENGTH
from .text import clean_column_name, ensure_unique_columns


def _truncate_column_name(name: str, suffix: str = "") -> str:
    limit = MAX_GPKG_NAME_LENGTH - len(suffix)
    if limit < 1:
        limit = 1
    base = (name or "")[:limit]
    return f"{base}{suffix}"


def _make_gpkg_safe_columns(columns: list[str], geometry_name: str | None = None) -> list[str]:
    """Make column names unique for GeoPackage writes, including case-insensitive collisions."""
    raw_cols: list[str] = []
    for col in columns:
        if geometry_name is not None and col == geometry_name:
            raw_cols.append(col)
            continue
        cleaned = clean_column_name(col)
        raw_cols.append(_truncate_column_name(cleaned))

    raw_cols = ensure_unique_columns(raw_cols)

    used_casefold: set[str] = set()
    safe_cols: list[str] = []
    for col in raw_cols:
        if geometry_name is not None and col == geometry_name:
            safe_cols.append(col)
            continue
        candidate = col or ""
        idx = 1
        while candidate.casefold() in used_casefold:
            idx += 1
            candidate = _truncate_column_name(col or "", f"_{idx}")
        used_casefold.add(candidate.casefold())
        safe_cols.append(candidate)
    return safe_cols


def ensure_valid_gpkg_dtypes(series: pd.Series) -> pd.Series:
    """Coerce pandas series into GeoPackage-safe scalar dtypes."""
    if pd.api.types.is_datetime64tz_dtype(series):
        series = series.dt.tz_localize(None)
    elif pd.api.types.is_timedelta64_dtype(series):
        series = series.astype(str)

    if pd.api.types.is_numeric_dtype(series):
        if pd.api.types.is_integer_dtype(series):
            dtype_name = str(series.dtype)
            if "int8" in dtype_name.lower():
                return series.astype("Int8" if "u" not in dtype_name.lower() else "UInt8")
            if "int16" in dtype_name.lower():
                return series.astype("Int16" if "u" not in dtype_name.lower() else "UInt16")
            if "int32" in dtype_name.lower():
                return series.astype("Int32" if "u" not in dtype_name.lower() else "UInt32")
            if "int64" in dtype_name.lower():
                return series.astype("Int64" if "u" not in dtype_name.lower() else "UInt64")
            return series.astype("Int64")
        return series.astype("float64")

    if pd.api.types.is_object_dtype(series) or any(isinstance(value, (list, dict, set, tuple)) for value in series.dropna().head(5)):
        series = series.apply(lambda value: str(value) if value is not None else None)

    return series


def sanitize_gdf_for_gpkg(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize column names and dtypes before writing a GeoPackage."""
    out = gdf.copy()
    geometry_name = out.geometry.name

    out.columns = _make_gpkg_safe_columns(list(out.columns), geometry_name=geometry_name)

    for col in out.columns:
        if col == geometry_name:
            continue
        series = out[col]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        series = ensure_valid_gpkg_dtypes(series)
        mask = pd.isna(series)
        if bool(mask.any()) and not pd.api.types.is_numeric_dtype(series):
            series = series.astype(object)
            series[mask] = None
        out[col] = series

    return out


def write_aspatial_gpkg_layer(df: pd.DataFrame, out_path: Path, layer_name: str) -> None:
    """Write a pandas DataFrame as an aspatial GeoPackage layer."""
    if df is None or df.empty and len(df.columns) == 0:
        raise ValueError("Cannot write an empty aspatial table with no columns.")
    safe_df = df.copy()
    safe_df.columns = _make_gpkg_safe_columns(list(safe_df.columns))
    safe_df = safe_df.where(pd.notna(safe_df), None)

    import pyogrio

    pyogrio.write_dataframe(safe_df, out_path, driver="GPKG", layer=layer_name)


def derive_layer_name_from_filename(name: str) -> str:
    """Build a stable GeoPackage layer name from a filename or label."""
    base = Path(name).stem.strip() or "dataset"
    base = base.replace(" ", "_").lower()
    if len(base) > MAX_GPKG_NAME_LENGTH:
        base = base[:MAX_GPKG_NAME_LENGTH]
    return base
