import statistics
from typing import Any

import geopandas as gpd
import pandas as pd
import streamlit as st

from .geopackage import ensure_valid_gpkg_dtypes
from .text import (
    clean_column_name,
    ensure_unique_columns,
    normalize_for_compare,
    normalize_value_for_compare,
    strip_unicode_spaces,
)


def detect_normalized_collisions(series: pd.Series) -> dict[str, set[str]]:
    """
    Return mapping of normalized value -> set of distinct raw values when
    multiple different raw values collapse to the same normalized key.
    """
    collisions: dict[str, set[str]] = {}
    try:
        for value in series.dropna():
            normalized = normalize_value_for_compare(value)
            if not normalized:
                continue
            bucket = collisions.setdefault(normalized, set())
            bucket.add(str(value))
        return {norm: raw_vals for norm, raw_vals in collisions.items() if len(raw_vals) > 1}
    except Exception:
        return {}


def detect_substation_column(df: pd.DataFrame) -> str | None:
    """
    Detect the correct substation column automatically.
    Uses header aliases + value heuristics to be resilient to naming drift.
    """
    if df.empty:
        return None

    alias_scores = {
        "substationname": 100,
        "substationnames": 95,
        "substation": 90,
        "substations": 90,
        "substationid": 70,
        "substationnameid": 68,
        "substationidentifier": 65,
        "substationnameprimary": 64,
        "primarysubstationname": 64,
        "substationprimaryname": 64,
        "nameofsubstation": 75,
        "stationname": 60,
    }

    def header_score(col: str) -> int:
        normalized = normalize_for_compare(strip_unicode_spaces(col))
        if not normalized:
            return 0
        if normalized in alias_scores:
            return alias_scores[normalized]
        if "substation" in normalized and "name" in normalized:
            return 80
        if normalized.startswith("substation"):
            return 70
        if "substation" in normalized:
            return 60
        if "station" in normalized and "name" in normalized:
            return 55
        return 0

    def value_score(series: pd.Series) -> float:
        sample = series.dropna().head(200)
        if sample.empty:
            return 0.0

        norm_vals = [normalize_value_for_compare(value) for value in sample]
        norm_vals = [value for value in norm_vals if value]
        if not norm_vals:
            return 0.0

        alpha_flags = [any(ch.isalpha() for ch in value) for value in norm_vals]
        alpha_ratio = sum(alpha_flags) / len(alpha_flags) if alpha_flags else 0.0
        unique_count = len(set(norm_vals))

        lengths = [len(value) for value in norm_vals]
        median_len = statistics.median(lengths) if lengths else 0.0
        length_bonus = max(0.0, 10.0 - abs(median_len - 12.0))

        return alpha_ratio * 40.0 + min(unique_count, 40) + length_bonus

    candidates: list[tuple[float, int, float, str]] = []
    for col in df.columns:
        h_score = header_score(col)
        v_score = value_score(df[col])
        total = h_score * 5 + v_score
        if total > 0:
            candidates.append((total, h_score, v_score, col))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (-item[0], -item[1], -item[2], len(normalize_for_compare(item[3]))))
    return candidates[0][3]


def forward_fill_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Forward-fill a specific column, treating blanks/whitespace as missing."""
    if df.empty or column not in df.columns:
        return df
    series = df[column].apply(strip_unicode_spaces)
    series = series.replace("", pd.NA)
    df[column] = series.ffill()
    return df


def st_dataframe_safe(df: Any, rows: int | None = None) -> None:
    """Render dataframes safely in Streamlit by stringifying geometry columns to avoid Arrow errors."""
    try:
        preview = df.head(rows) if rows else df
        if hasattr(preview, "geometry"):
            preview = preview.copy()
            geom_col = preview.geometry.name
            preview[geom_col] = preview[geom_col].apply(lambda geom: getattr(geom, "wkt", None) if geom is not None else None)
        elif "geometry" in preview.columns:
            preview = preview.copy()
            preview["geometry"] = preview["geometry"].apply(
                lambda geom: getattr(geom, "wkt", None) if hasattr(geom, "wkt") else str(geom)
            )
        st.dataframe(preview)
    except Exception:
        st.dataframe(df)


def merge_without_duplicates(gdf: gpd.GeoDataFrame, df: pd.DataFrame, left_key: str, right_key: str) -> gpd.GeoDataFrame:
    """
    Join df onto gdf with Excel values overwriting GeoPackage values when matched.
    Uses normalized key lookup instead of pandas merge to avoid ambiguous truthiness
    and to better control column handling.
    """
    base = gdf.copy()
    incoming = df.copy()

    geometry_name = base.geometry.name if hasattr(base, "geometry") else None

    incoming.columns = ensure_unique_columns([clean_column_name(col) for col in incoming.columns])

    left_collisions = detect_normalized_collisions(base[left_key])
    right_collisions = detect_normalized_collisions(incoming[right_key])
    if left_collisions or right_collisions:
        examples = []
        if left_collisions:
            examples.append(
                "GeoPackage join field has duplicate normalized keys "
                + "; ".join(", ".join(sorted(vals)) for vals in left_collisions.values())
            )
        if right_collisions:
            examples.append(
                "Excel join field has duplicate normalized keys "
                + "; ".join(", ".join(sorted(vals)) for vals in right_collisions.values())
            )
        raise ValueError(". ".join(examples))

    base_norm = base[left_key].map(normalize_value_for_compare)
    incoming_norm = incoming[right_key].map(normalize_value_for_compare)
    incoming[nk := "_norm_key"] = incoming_norm

    incoming_dicts = {col: incoming.set_index(nk)[col].to_dict() for col in incoming.columns if col != nk}

    gpkg_norm = {
        normalize_for_compare(col): col
        for col in base.columns
        if col != geometry_name
    }
    normalized_matches: dict[str, str] = {}
    for col in incoming.columns:
        if col == right_key or col == nk:
            continue
        norm = normalize_for_compare(col)
        if norm in gpkg_norm:
            normalized_matches[col] = gpkg_norm[norm]

    for col in incoming.columns:
        if col in (right_key, nk):
            continue
        target_col = normalized_matches.get(col, col)
        if target_col == geometry_name:
            continue
        if target_col not in base.columns:
            base[target_col] = pd.NA
        mapping = incoming_dicts.get(col, {})
        base[target_col] = base_norm.map(mapping).where(base_norm.map(mapping).notna(), base.get(target_col))
        base[target_col] = ensure_valid_gpkg_dtypes(base[target_col])

    if nk in base.columns:
        base.drop(columns=[nk], inplace=True, errors="ignore")

    return gpd.GeoDataFrame(base, geometry=geometry_name, crs=gdf.crs)
