from pathlib import Path

import geopandas as gpd
import pandas as pd

from .config import MAX_GPKG_NAME_LENGTH
from .text import clean_column_name, ensure_unique_columns


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

    new_cols: list[str] = []
    for col in out.columns:
        if col == geometry_name:
            new_cols.append(col)
            continue
        cleaned = clean_column_name(col)
        if len(cleaned) > MAX_GPKG_NAME_LENGTH:
            cleaned = cleaned[:MAX_GPKG_NAME_LENGTH]
        new_cols.append(cleaned)

    out.columns = ensure_unique_columns(new_cols)

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
