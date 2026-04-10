from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from .text import normalize_for_compare, normalize_value_for_compare


def _norm_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_value_for_compare)


def _nonempty_norm_values(series: pd.Series) -> list[str]:
    return [value for value in _norm_series(series).tolist() if value]


def _duplicate_norm_samples(series: pd.Series, limit: int = 10) -> list[str]:
    values = _nonempty_norm_values(series)
    counts = pd.Series(values).value_counts()
    return counts[counts > 1].index.astype(str).tolist()[:limit]


def build_dataset_profile(
    data: pd.DataFrame | gpd.GeoDataFrame,
    *,
    name: str | None = None,
    layer_name: str | None = None,
    source_format: str | None = None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "name": name or "",
        "layer_name": layer_name or "",
        "source_format": source_format or "",
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "columns": [str(col) for col in data.columns],
    }
    if hasattr(data, "geometry"):
        geom_name = data.geometry.name
        profile["geometry_column"] = geom_name
        try:
            profile["geometry_types"] = data.geometry.geom_type.dropna().astype(str).value_counts().to_dict()
        except Exception:
            profile["geometry_types"] = {}
        try:
            profile["crs"] = str(data.crs) if data.crs is not None else ""
        except Exception:
            profile["crs"] = ""
    else:
        profile["geometry_column"] = ""
        profile["geometry_types"] = {}
        profile["crs"] = ""
    return profile


def build_join_validations(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str | None,
    right_key: str | None,
    *,
    left_label: str = "left",
    right_label: str = "right",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if left_key not in left_df.columns:
        rows.append(
            {
                "rule": "left_join_key_exists",
                "status": "fail",
                "message": f"{left_label} join key '{left_key}' is missing.",
                "context": {"left_key": left_key, "left_label": left_label},
            }
        )
        return rows
    rows.append(
        {
            "rule": "left_join_key_exists",
            "status": "pass",
            "message": f"{left_label} join key '{left_key}' exists.",
            "context": {"left_key": left_key, "left_label": left_label},
        }
    )

    if right_key not in right_df.columns:
        rows.append(
            {
                "rule": "right_join_key_exists",
                "status": "fail",
                "message": f"{right_label} join key '{right_key}' is missing.",
                "context": {"right_key": right_key, "right_label": right_label},
            }
        )
        return rows
    rows.append(
        {
            "rule": "right_join_key_exists",
            "status": "pass",
            "message": f"{right_label} join key '{right_key}' exists.",
            "context": {"right_key": right_key, "right_label": right_label},
        }
    )

    left_values = _nonempty_norm_values(left_df[left_key])
    right_values = _nonempty_norm_values(right_df[right_key])
    left_unique = set(left_values)
    right_unique = set(right_values)
    overlap = left_unique & right_unique

    rows.append(
        {
            "rule": "left_join_values_present",
            "status": "pass" if left_values else "warn",
            "message": f"{left_label} has {len(left_unique)} non-empty normalized join key(s).",
            "context": {"left_key": left_key, "left_label": left_label},
            "metrics": {"unique_key_count": len(left_unique)},
        }
    )
    rows.append(
        {
            "rule": "right_join_values_present",
            "status": "pass" if right_values else "warn",
            "message": f"{right_label} has {len(right_unique)} non-empty normalized join key(s).",
            "context": {"right_key": right_key, "right_label": right_label},
            "metrics": {"unique_key_count": len(right_unique)},
        }
    )

    left_duplicates = _duplicate_norm_samples(left_df[left_key])
    right_duplicates = _duplicate_norm_samples(right_df[right_key])
    rows.append(
        {
            "rule": "left_join_key_duplicates",
            "status": "warn" if left_duplicates else "pass",
            "message": (
                f"{left_label} has duplicate normalized join keys."
                if left_duplicates
                else f"{left_label} has no duplicate normalized join keys."
            ),
            "context": {"left_key": left_key, "left_label": left_label, "samples": left_duplicates},
            "metrics": {"duplicate_key_count": len(left_duplicates)},
        }
    )
    rows.append(
        {
            "rule": "right_join_key_duplicates",
            "status": "warn" if right_duplicates else "pass",
            "message": (
                f"{right_label} has duplicate normalized join keys."
                if right_duplicates
                else f"{right_label} has no duplicate normalized join keys."
            ),
            "context": {"right_key": right_key, "right_label": right_label, "samples": right_duplicates},
            "metrics": {"duplicate_key_count": len(right_duplicates)},
        }
    )
    rows.append(
        {
            "rule": "join_key_overlap",
            "status": "pass" if overlap else "warn",
            "message": f"{len(overlap)} normalized join key(s) overlap between {left_label} and {right_label}.",
            "context": {"left_key": left_key, "right_key": right_key},
            "metrics": {
                "overlap_count": len(overlap),
                "left_unique_count": len(left_unique),
                "right_unique_count": len(right_unique),
            },
        }
    )
    return rows


def build_field_mapping_rows(
    source_columns: list[str],
    target_fields: list[str],
    selected_mapping: dict[str, Any] | None,
    *,
    suggested_mapping: dict[str, Any] | None = None,
    score_map: dict[str, float] | None = None,
    geometry_name: str | None = None,
    low_confidence_threshold: float = 0.6,
) -> list[dict[str, Any]]:
    available_sources = {str(col) for col in source_columns if geometry_name is None or str(col) != geometry_name}
    rows: list[dict[str, Any]] = []
    selected_mapping = selected_mapping or {}
    suggested_mapping = suggested_mapping or {}
    score_map = score_map or {}
    for target in target_fields:
        raw_selected = selected_mapping.get(target)
        selected = None if raw_selected in (None, "", "(empty)") else str(raw_selected)
        if selected not in available_sources:
            selected = None
        suggested = suggested_mapping.get(target)
        suggested = None if suggested in (None, "", "(empty)") else str(suggested)
        if suggested not in available_sources:
            suggested = None
        score = float(score_map.get(target, 0.0) or 0.0)
        if selected is None:
            status = "unmapped"
        elif score and score < low_confidence_threshold:
            status = "low_confidence"
        else:
            status = "mapped"
        rows.append(
            {
                "target_field": target,
                "selected_source": selected or "",
                "suggested_source": suggested or "",
                "score": score,
                "status": status,
                "auto_selected": bool(selected and suggested and normalize_for_compare(selected) == normalize_for_compare(suggested)),
            }
        )
    return rows


def build_mapping_validations(
    mapping_rows: list[dict[str, Any]],
    *,
    low_confidence_threshold: float = 0.6,
) -> list[dict[str, Any]]:
    mapped = [row for row in mapping_rows if row.get("selected_source")]
    unmapped = [row for row in mapping_rows if not row.get("selected_source")]
    low_conf = [
        row
        for row in mapping_rows
        if row.get("selected_source") and float(row.get("score") or 0.0) < low_confidence_threshold
    ]
    selected_sources = [str(row.get("selected_source")) for row in mapped if row.get("selected_source")]
    duplicate_source_counts = pd.Series(selected_sources).value_counts() if selected_sources else pd.Series(dtype="int64")
    reused_sources = duplicate_source_counts[duplicate_source_counts > 1].to_dict()

    return [
        {
            "rule": "schema_fields_mapped",
            "status": "pass" if mapped else "warn",
            "message": f"{len(mapped)} of {len(mapping_rows)} schema field(s) are mapped.",
            "metrics": {"mapped_count": len(mapped), "target_field_count": len(mapping_rows)},
        },
        {
            "rule": "schema_fields_unmapped",
            "status": "warn" if unmapped else "pass",
            "message": f"{len(unmapped)} schema field(s) are unmapped.",
            "context": {"fields": [row.get("target_field") for row in unmapped[:25]]},
            "metrics": {"unmapped_count": len(unmapped)},
        },
        {
            "rule": "mapping_low_confidence",
            "status": "warn" if low_conf else "pass",
            "message": f"{len(low_conf)} mapped field(s) are below the confidence threshold {low_confidence_threshold:.2f}.",
            "context": {"fields": [row.get("target_field") for row in low_conf[:25]]},
            "metrics": {"low_confidence_count": len(low_conf), "threshold": low_confidence_threshold},
        },
        {
            "rule": "source_column_reuse",
            "status": "warn" if reused_sources else "pass",
            "message": "Some source columns are reused across multiple target fields." if reused_sources else "No source columns are reused across target fields.",
            "context": {"reused_sources": reused_sources},
            "metrics": {"reused_source_count": len(reused_sources)},
        },
    ]


def build_output_validations(
    out_df: pd.DataFrame | gpd.GeoDataFrame,
    *,
    expected_fields: list[str] | None = None,
    geometry_required: bool = False,
    label: str = "output",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [
        {
            "rule": "output_rows_nonzero",
            "status": "pass" if len(out_df) > 0 else "warn",
            "message": f"{label} contains {len(out_df)} row(s).",
            "metrics": {"row_count": int(len(out_df))},
        }
    ]
    if expected_fields:
        missing = [field for field in expected_fields if field not in out_df.columns]
        rows.append(
            {
                "rule": "expected_fields_present",
                "status": "fail" if missing else "pass",
                "message": (
                    f"{len(missing)} expected field(s) are missing from {label}."
                    if missing
                    else f"All expected fields are present in {label}."
                ),
                "context": {"missing_fields": missing[:50]},
                "metrics": {"missing_field_count": len(missing), "expected_field_count": len(expected_fields)},
            }
        )
    if geometry_required:
        has_geometry = hasattr(out_df, "geometry")
        rows.append(
            {
                "rule": "geometry_present",
                "status": "pass" if has_geometry else "fail",
                "message": f"{label} {'has' if has_geometry else 'does not have'} geometry.",
            }
        )
    return rows


def build_rewritten_id_validations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("status") or "")
        validation_status = "pass" if status == "ok" else "fail" if status in {"output_missing", "output_read_failed"} else "warn"
        checks.append(
            {
                "rule": "rewritten_id_validation",
                "status": validation_status,
                "message": (
                    f"{row.get('device', 'device')}: rewritten ID validation status is {status or 'unknown'}."
                ),
                "context": {
                    "device": row.get("device"),
                    "output_file": row.get("output_file"),
                    "output_id_column": row.get("output_id_column"),
                },
                "metrics": {
                    "workbook_id_count": row.get("workbook_id_count", 0),
                    "output_id_count": row.get("output_id_count", 0),
                    "missing_workbook_id_count": row.get("missing_workbook_id_count", 0),
                    "extra_output_id_count": row.get("extra_output_id_count", 0),
                },
            }
        )
    return checks
