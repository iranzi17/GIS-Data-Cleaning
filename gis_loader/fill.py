import csv
import io
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from gis_loader.config import (
    DOMAIN_CODE_LOG_FILE,
    EARTHING_TRANSFORMER_TEMPLATE_PATH,
    HV_LINE_TEMPLATE_PATH,
)
from gis_loader.data_sources import coerce_gpkg_path, get_file_name, list_gpkg_layers
from gis_loader.equipment import PROTECTION_LAYOUT_DEVICES, resolve_equipment_name
from gis_loader.geopackage import (
    derive_layer_name_from_filename,
    sanitize_gdf_for_gpkg,
    write_aspatial_gpkg_layer,
)
from gis_loader.schema import coerce_series_to_type
from gis_loader.supervisor import parse_supervisor_device_table
from gis_loader.text import normalize_for_compare, normalize_value_for_compare


def _build_domain_log_rows(entries: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    if not entries:
        return []
    ts = datetime.now().isoformat(timespec="seconds")
    rows: list[dict[str, Any]] = []
    for entry in entries:
        rows.append(
            {
                "timestamp": ts,
                "workbook": context.get("workbook"),
                "sheet": context.get("sheet"),
                "device": context.get("device"),
                "output": context.get("output"),
                "field": entry.get("field"),
                "domain": entry.get("domain"),
                "code": entry.get("code"),
                "source": entry.get("source"),
            }
        )
    return rows


def domain_log_rows_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    headers = [
        "timestamp",
        "workbook",
        "sheet",
        "device",
        "output",
        "field",
        "domain",
        "code",
        "source",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in headers})
    return buf.getvalue()


def append_domain_code_log(entries: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    """Append domain-code usage entries to a JSONL log file and return rows."""
    rows = _build_domain_log_rows(entries, context)
    if not rows:
        return []
    try:
        lines = [json.dumps(row, ensure_ascii=False) for row in rows]
        with open(DOMAIN_CODE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception:
        # best-effort logging; never break the app
        pass
    return rows


def collect_domain_log_entries(instances: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not instances:
        return []
    seen = set()
    out: list[dict[str, Any]] = []
    for inst in instances:
        for entry in inst.get("domain_log", []) or []:
            key = (
                entry.get("field"),
                entry.get("domain"),
                entry.get("code"),
                entry.get("source"),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(entry)
    return out


def build_device_gdf_from_instances(
    instances: list[dict[str, Any]],
    points: list[Any],
    crs,
) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame with instance fields aligned to generated points."""
    count = len(points)
    if count <= 0:
        return gpd.GeoDataFrame(geometry=[])
    type_map_local: dict[str, str] = {}
    for inst in instances:
        tm = inst.get("type_map") if isinstance(inst, dict) else None
        if isinstance(tm, dict) and tm:
            type_map_local = dict(tm)
            break
    fields_ordered: list[str] = []
    fields_seen: set[str] = set()
    for inst in instances:
        for field_name in inst.get("order", []) or []:
            if field_name not in fields_seen:
                fields_seen.add(field_name)
                fields_ordered.append(field_name)
        for field_name in (inst.get("fields", {}) or {}).keys():
            if field_name not in fields_seen:
                fields_seen.add(field_name)
                fields_ordered.append(field_name)
    data: dict[str, list[Any]] = {field_name: [pd.NA] * count for field_name in fields_ordered}
    for idx, inst in enumerate(instances):
        if idx >= count:
            break
        fields = inst.get("fields", {}) or {}
        for field_name, value in fields.items():
            if field_name not in data:
                data[field_name] = [pd.NA] * count
            fill_val = value.iloc[0] if isinstance(value, pd.Series) else value
            data[field_name][idx] = fill_val
    out_gdf = gpd.GeoDataFrame(data, geometry=points, crs=crs)
    if type_map_local:
        norm_lookup = {normalize_for_compare(key): value for key, value in type_map_local.items() if value is not None}
        for col in out_gdf.columns:
            if col == out_gdf.geometry.name:
                continue
            type_str = type_map_local.get(col) or norm_lookup.get(normalize_for_compare(col))
            if type_str:
                try:
                    out_gdf[col] = coerce_series_to_type(out_gdf[col], type_str)
                except Exception:
                    pass
    return sanitize_gdf_for_gpkg(out_gdf)


def build_device_table_from_instances(instances: list[dict[str, Any]]) -> pd.DataFrame:
    """Build a non-spatial attribute table from supervisor instances."""
    count = len(instances)
    if count <= 0:
        return pd.DataFrame()
    type_map_local: dict[str, str] = {}
    for inst in instances:
        tm = inst.get("type_map") if isinstance(inst, dict) else None
        if isinstance(tm, dict) and tm:
            type_map_local = dict(tm)
            break
    fields_ordered: list[str] = []
    fields_seen: set[str] = set()
    for inst in instances:
        for field_name in inst.get("order", []) or []:
            if field_name not in fields_seen:
                fields_seen.add(field_name)
                fields_ordered.append(field_name)
        for field_name in (inst.get("fields", {}) or {}).keys():
            if field_name not in fields_seen:
                fields_seen.add(field_name)
                fields_ordered.append(field_name)
    data: dict[str, list[Any]] = {field_name: [pd.NA] * count for field_name in fields_ordered}
    for idx, inst in enumerate(instances):
        fields = inst.get("fields", {}) or {}
        for field_name, value in fields.items():
            if field_name not in data:
                data[field_name] = [pd.NA] * count
            fill_val = value.iloc[0] if isinstance(value, pd.Series) else value
            data[field_name][idx] = fill_val
    out_df = pd.DataFrame(data)
    if type_map_local:
        norm_lookup = {normalize_for_compare(key): value for key, value in type_map_local.items() if value is not None}
        for col in out_df.columns:
            type_str = type_map_local.get(col) or norm_lookup.get(normalize_for_compare(col))
            if type_str:
                try:
                    out_df[col] = coerce_series_to_type(out_df[col], type_str)
                except Exception:
                    pass
    return out_df


def repeat_instances(instances: list[dict[str, Any]], repeat_count: int) -> list[dict[str, Any]]:
    """Repeat each instance in order to match a target count per instance."""
    if repeat_count <= 0:
        return []
    expanded: list[dict[str, Any]] = []
    for inst in instances:
        for _ in range(repeat_count):
            expanded.append(inst)
    return expanded


def split_instance_prefix_suffix(value: Any) -> tuple[str | None, int | None]:
    """Split an instance label into prefix and numeric suffix (e.g., Q1-3 -> Q1, 3)."""
    if value is None:
        return None, None
    try:
        if pd.isna(value):
            return None, None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None, None
    match = re.match(r"^([A-Za-z]+\d+)[-_ ]+(\d+)$", text)
    if not match:
        return None, None
    prefix = match.group(1).strip()
    try:
        suffix = int(match.group(2))
    except Exception:
        suffix = None
    return prefix, suffix


def build_spatial_match_targets(
    line_gdf: gpd.GeoDataFrame,
    bay_path: Path,
    bay_layer: str | None,
    bay_field: str | None,
    allow_nearest_fallback: bool = True,
) -> pd.Series:
    """Return normalized match targets for each line feature based on Line Bay polygons."""
    if line_gdf is None or line_gdf.empty or bay_path is None or bay_layer is None or bay_field is None:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)
    try:
        bay_gdf = gpd.read_file(bay_path, layer=bay_layer)
    except Exception:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)
    if not hasattr(bay_gdf, "geometry"):
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)

    geom_name = bay_gdf.geometry.name
    norm_lookup = {normalize_for_compare(col): col for col in bay_gdf.columns}
    key_col = None
    for alias in ["Line_Bay_ID", "Line Bay ID", "LineBayID", "LineBay_ID", "Line_BayID", "line_bay_id"]:
        col = norm_lookup.get(normalize_for_compare(alias))
        if col:
            key_col = col
            break
    if key_col is None and bay_field in bay_gdf.columns:
        key_col = bay_field
    if key_col is None:
        fallback_cols = [col for col in bay_gdf.columns if col != geom_name]
        key_col = fallback_cols[0] if fallback_cols else None
    if key_col is None:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)

    keep_cols = [geom_name, key_col]
    if bay_field in bay_gdf.columns and bay_field not in keep_cols:
        keep_cols.append(bay_field)
    bay = bay_gdf[keep_cols].copy()
    try:
        bay = bay[bay[geom_name].notna() & ~bay[geom_name].is_empty]
    except Exception:
        pass
    try:
        bay_keys_norm = bay[key_col].map(normalize_value_for_compare)
        bay = bay[bay_keys_norm != ""]
    except Exception:
        pass
    if line_gdf.crs is not None and bay.crs is not None and line_gdf.crs != bay.crs:
        try:
            bay = bay.to_crs(line_gdf.crs)
        except Exception:
            pass

    try:
        joined = gpd.sjoin(line_gdf, bay, how="left", predicate="intersects", rsuffix="bay")
    except TypeError:
        joined = gpd.sjoin(line_gdf, bay, how="left", op="intersects", rsuffix="bay")
    except Exception:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)

    field_name = key_col
    if field_name not in joined.columns:
        alt = f"{key_col}_bay"
        field_name = alt if alt in joined.columns else key_col

    if joined.index.duplicated().any():
        try:
            joined["_left_index"] = joined.index
            right_geom = bay.geometry

            def _inter_len(row: pd.Series) -> float:
                try:
                    idx_right = row.get("index_right")
                    if pd.isna(idx_right):
                        return 0.0
                    return row.geometry.intersection(right_geom.loc[idx_right]).length
                except Exception:
                    return 0.0

            joined["__inter_len__"] = joined.apply(_inter_len, axis=1)
            joined = joined.sort_values("__inter_len__", ascending=False).drop_duplicates(subset="_left_index")
            joined = joined.set_index("_left_index")
        except Exception:
            joined = joined[~joined.index.duplicated(keep="first")]

    series = joined[field_name] if field_name in joined.columns else pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)
    series = series.reindex(line_gdf.index)

    if not allow_nearest_fallback:
        return series.map(normalize_value_for_compare)

    try:
        missing_mask = series.isna() | series.map(lambda value: normalize_value_for_compare(value) == "")
    except Exception:
        missing_mask = series.isna()

    if bool(missing_mask.any()):
        try:
            bay_geom_name = bay.geometry.name if hasattr(bay, "geometry") else geom_name
            bay_refs: list[tuple[Any, Any]] = []
            for _, bay_row in bay.iterrows():
                key_val = bay_row.get(key_col)
                geom = bay_row.get(bay_geom_name)
                if geom is None or getattr(geom, "is_empty", True):
                    continue
                try:
                    ref_pt = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
                except Exception:
                    try:
                        ref_pt = geom.representative_point()
                    except Exception:
                        continue
                if ref_pt is None or getattr(ref_pt, "is_empty", True):
                    continue
                bay_refs.append((key_val, ref_pt))

            if bay_refs and hasattr(line_gdf, "geometry") and line_gdf.geometry is not None:
                for idx_val in series.index[missing_mask]:
                    try:
                        geom = line_gdf.loc[idx_val, line_gdf.geometry.name]
                    except Exception:
                        continue
                    if geom is None or getattr(geom, "is_empty", True):
                        continue
                    try:
                        src_pt = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
                    except Exception:
                        continue
                    best_key = None
                    best_dist = None
                    for key_val, ref_pt in bay_refs:
                        try:
                            dist = src_pt.distance(ref_pt)
                        except Exception:
                            continue
                        if best_dist is None or dist < best_dist:
                            best_dist = dist
                            best_key = key_val
                    if best_key is not None:
                        series.at[idx_val] = best_key
        except Exception:
            pass

    return series.map(normalize_value_for_compare)


def ensure_name_fields_string(gdf: gpd.GeoDataFrame, fields: list[str]) -> gpd.GeoDataFrame:
    """Force name-like fields to string dtype to avoid GPKG schema errors."""
    for col in fields:
        if col in gdf.columns:
            try:
                gdf[col] = gdf[col].astype("string")
            except Exception:
                try:
                    gdf[col] = gdf[col].astype(str)
                except Exception:
                    pass
    return gdf


_NUM_REGEX = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def id_validation_rows_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    fieldnames = sorted({key for row in rows for key in row.keys()})
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def rewritten_id_validation_specs() -> list[dict[str, Any]]:
    return [
        {
            "device_name": "Voltage Transformer",
            "output_id_aliases": [
                "VoltageTransfomer_ID",
                "VoltageTransformer_ID",
                "Voltage Transformer ID",
            ],
        },
        {
            "device_name": "Current Transformer",
            "output_id_aliases": [
                "Current Transfomer ID",
                "CurrentTransformer_ID",
                "CurrentTransformerID",
                "Current Transformer ID",
            ],
        },
        {
            "device_name": "Lightning Arrester",
            "output_id_aliases": [
                "ArresterID",
                "Arrester_ID",
                "Arrester ID",
                "LightningArresterID",
                "Lightning Arrester ID",
            ],
        },
        {
            "device_name": "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
            "output_id_aliases": [
                "CircuitBreakerID",
                "Circuit_Breaker_ID",
                "Circuit Breaker ID",
            ],
        },
        {
            "device_name": "High Voltage Switch/High Voltage Switch",
            "output_id_aliases": [
                "HV_Switch_ID",
                "HV Switch ID",
                "HVSwitchID",
                "Disconnector_ID",
                "Disconnector ID",
            ],
        },
    ]


def get_rewritten_id_validation_spec(device_name: Any) -> dict[str, Any] | None:
    dev_norm = normalize_for_compare(device_name)
    if not dev_norm:
        return None
    for spec in rewritten_id_validation_specs():
        if normalize_for_compare(spec.get("device_name")) == dev_norm:
            return spec
    return None


def dedupe_id_texts(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        try:
            if value is None or pd.isna(value):
                continue
        except Exception:
            if value is None:
                continue
        text = str(value).strip()
        if not text:
            continue
        norm = normalize_value_for_compare(text)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(text)
    return out


def pick_existing_column_name(columns: list[str], aliases: list[str]) -> str | None:
    lookup = {normalize_for_compare(col): col for col in columns}
    for alias in aliases:
        col = lookup.get(normalize_for_compare(alias))
        if col:
            return col
    return None


def read_output_ids_for_validation(out_path: Path, id_aliases: list[str]) -> tuple[list[str], str | None, str | None]:
    try:
        layers = list_gpkg_layers(out_path)
    except Exception:
        layers = []
    try:
        if layers:
            out_gdf = gpd.read_file(out_path, layer=layers[0])
        else:
            out_gdf = gpd.read_file(out_path)
    except Exception as exc:
        return [], None, f"{type(exc).__name__}: {exc}"
    id_col = pick_existing_column_name(list(out_gdf.columns), id_aliases)
    if not id_col:
        return [], None, "id column not found"
    return dedupe_id_texts(out_gdf[id_col].tolist()), id_col, None


def build_rewritten_id_validation_row(
    substation_name: str | None,
    workbook_path: Path | None,
    sheet_name: str | None,
    device_name: str,
    output_name: str | None,
    out_path: Path | None,
    instances: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    spec = get_rewritten_id_validation_spec(device_name)
    if spec is None:
        return None

    workbook_ids = dedupe_id_texts(
        [inst.get("id_value") for inst in (instances or []) if isinstance(inst, dict)]
    )
    output_ids: list[str] = []
    output_id_col = None
    read_error = None
    if out_path is not None:
        output_ids, output_id_col, read_error = read_output_ids_for_validation(
            out_path,
            spec.get("output_id_aliases", []),
        )

    workbook_lookup = {normalize_value_for_compare(val): val for val in workbook_ids}
    output_lookup = {normalize_value_for_compare(val): val for val in output_ids}
    missing_workbook_ids = [workbook_lookup[norm] for norm in workbook_lookup if norm not in output_lookup]
    extra_output_ids = [output_lookup[norm] for norm in output_lookup if norm not in workbook_lookup]

    if out_path is None:
        status = "output_missing"
    elif read_error:
        status = "output_read_failed"
    elif not workbook_ids and output_ids:
        status = "workbook_ids_missing"
    elif extra_output_ids and missing_workbook_ids:
        status = "extra_and_missing"
    elif extra_output_ids:
        status = "extra_output_ids"
    elif missing_workbook_ids:
        status = "missing_workbook_ids"
    else:
        status = "ok"

    return {
        "substation": substation_name or "",
        "workbook": workbook_path.name if workbook_path else "",
        "sheet": sheet_name or "",
        "device": device_name,
        "output_file": output_name or "",
        "output_id_column": output_id_col or "",
        "status": status,
        "workbook_id_count": len(workbook_ids),
        "output_id_count": len(output_ids),
        "missing_workbook_id_count": len(missing_workbook_ids),
        "extra_output_id_count": len(extra_output_ids),
        "workbook_ids": "; ".join(workbook_ids),
        "output_ids": "; ".join(output_ids),
        "missing_workbook_ids": "; ".join(missing_workbook_ids),
        "extra_output_ids": "; ".join(extra_output_ids),
        "read_error": read_error or "",
    }


DROP_OUTPUT_COLUMNS = {
    normalize_for_compare("Composite_ID"),
    normalize_for_compare("Composite ID"),
}

SEQUENTIAL_FILL_DEVICES = {
    normalize_for_compare("Indoor Circuit Breaker/30kv/15kb"),
    normalize_for_compare("Indoor Current Transformer"),
    normalize_for_compare("Indoor Voltage Transformer"),
}

BLOCK_ASSIGN_DEVICES = {
    normalize_for_compare("High Voltage Line"),
    normalize_for_compare("High Voltage Circuit Breaker/High Voltage Circuit Breaker"),
}

LINE_BAY_SPATIAL_DEVICES = {
    normalize_for_compare("High Voltage Line"),
}

PREFIX_GROUP_DEVICES = {
    normalize_for_compare("High Voltage Switch/High Voltage Switch"),
}

ASPATIAL_DEVICES = {
    normalize_for_compare("Optical Telecommunication Equipment (Telecom)"),
    normalize_for_compare("ODF"),
}

FORCED_CABIN_AUTO_CREATE_DEVICES = {
    normalize_for_compare("Transformer Bay"),
}

SUBSTATION_PRESERVE_FIELDS = {
    normalize_for_compare("AssetGroup"),
    normalize_for_compare("Asset Group"),
    normalize_for_compare("AssetType"),
    normalize_for_compare("Asset Type"),
    normalize_for_compare("Substation_Name"),
    normalize_for_compare("Substation Name"),
}

SUBSTATION_PRESERVE_ORDER = [
    "AssetGroup",
    "AssetType",
    "Substation_Name",
]

SUBSTATION_FORCE_TYPES = {
    normalize_for_compare("AssetGroup"): "Long Integer",
    normalize_for_compare("Asset Group"): "Long Integer",
    normalize_for_compare("AssetType"): "Short Integer",
    normalize_for_compare("Asset Type"): "Short Integer",
    normalize_for_compare("Substation_Name"): "Short Integer",
    normalize_for_compare("Substation Name"): "Short Integer",
}

PROTECTION_LAYOUT_SPACING = 2.0

SKIP_BATCH_FILL_STEMS = {
    normalize_for_compare("connection points"),
    normalize_for_compare("connection_point"),
    normalize_for_compare("connection_points"),
    normalize_for_compare("connectionpoints"),
    normalize_for_compare("point connection"),
    normalize_for_compare("point connections"),
}


@dataclass(frozen=True)
class FillBatchDependencies:
    fill_one_gpkg: Any
    collect_device_polygons_from_uploads: Any
    build_cabin_anchor_points: Any
    build_control_panel_polygons: Any
    enrich_line_bay_reference_info: Any
    load_ups_anchor_and_crs: Any
    build_points_in_panel_polygons: Any
    build_protection_layout_points: Any
    layout_points_in_cabins: Any
    expand_geometries: Any
    load_template_layer: Any
    load_line_bay_layer: Any
    pick_line_bay_name_field: Any
    extract_bay_name_from_row: Any
    collect_device_points_from_uploads: Any
    collect_point_geometries_from_uploads: Any
    map_points_to_bays: Any
    build_lines_from_points_in_polygon: Any
    replace_line_name_ids: Any
    ensure_name_fields_string: Any


def fill_supervisor_batch(
    files: list[Any],
    device_options: list[str],
    sup_wb_path: Path,
    sup_sheet: str,
    equip_map_sup: dict[str, str],
    line_bay_info: dict[str, Any] | None,
    ups_anchor_info: dict[str, Any] | None,
    deps: FillBatchDependencies,
    seq_assign_fallback: bool = True,
    output_prefix: str | None = None,
    id_validation_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[tuple[str, Path]], list[str], list[dict[str, Any]]]:
    """Fill a batch of supervisor GeoPackages and return outputs, logs, and domain rows."""
    logs: list[str] = []
    outputs: list[tuple[str, Path]] = []
    instance_cache: dict[str, list[dict[str, Any]]] = {}
    uploaded_device_norms: set[str] = set()
    run_domain_rows: list[dict[str, Any]] = []
    validated_rewrite_devices: set[str] = set()
    prefix_label = f"{output_prefix}/" if output_prefix else ""
    substation_label = output_prefix or ""

    def _record_output(name: str, out_path: Path) -> None:
        arc_name = Path(name).name
        if output_prefix:
            arc_name = str(Path(output_prefix) / arc_name)
        outputs.append((arc_name, out_path))

    def _write_original_file(file_obj: Any) -> Path:
        gpkg_path = coerce_gpkg_path(file_obj)
        if gpkg_path is None:
            raise ValueError("Could not read GeoPackage.")
        return gpkg_path

    def _append_validation_log(row: dict[str, Any], output_name: str | None) -> None:
        status = row.get("status")
        if status == "ok":
            return
        label = f"{prefix_label}{output_name}" if output_name else f"{prefix_label}{row.get('device', 'device')}"
        logs.append(
            f"{label}: rewritten ID validation {status} "
            f"(missing workbook ids={row.get('missing_workbook_id_count', 0)}, "
            f"extra output ids={row.get('extra_output_id_count', 0)})."
        )

    def _validate_rewritten_output(
        device_name: str | None,
        output_name: str | None,
        out_path: Path | None,
        instances: list[dict[str, Any]] | None,
    ) -> None:
        spec = get_rewritten_id_validation_spec(device_name)
        if spec is None:
            return
        validated_rewrite_devices.add(normalize_for_compare(device_name))
        row = build_rewritten_id_validation_row(
            substation_label,
            sup_wb_path,
            sup_sheet,
            spec.get("device_name", device_name or ""),
            output_name,
            out_path,
            instances,
        )
        if row is None:
            return
        if id_validation_rows is not None:
            id_validation_rows.append(row)
        _append_validation_log(row, output_name)

    def _pick_instance_for_file(name: str, instances: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not instances:
            return None
        if len(instances) == 1:
            return instances[0]
        stem_norm = normalize_for_compare(Path(name).stem)
        for inst in instances:
            for cand in (inst.get("id_value"), inst.get("name_value"), inst.get("feeder_value")):
                if pd.notna(cand) and normalize_for_compare(cand) in stem_norm:
                    return inst
        return instances[0]

    cabin_norms = {normalize_for_compare("Substation/Cabin")}
    cabins_gdf_cached = deps.collect_device_polygons_from_uploads(
        files, None, device_options, equip_map_sup, cabin_norms
    )
    cabin_anchor_points_cached: list[Any] = []
    if cabins_gdf_cached is not None and not cabins_gdf_cached.empty:
        cabin_anchor_points_cached = deps.build_cabin_anchor_points(
            files,
            cabins_gdf_cached,
            device_options,
            equip_map_sup,
        )
    control_panel_instances_cached = parse_supervisor_device_table(
        sup_wb_path, sup_sheet, "Control and Protection Panels"
    )
    control_panel_polygons_cached: list[Any] = []
    if (
        cabins_gdf_cached is not None
        and not cabins_gdf_cached.empty
        and control_panel_instances_cached
    ):
        control_panel_polygons_cached = deps.build_control_panel_polygons(
            control_panel_instances_cached,
            cabins_gdf_cached,
            cabin_anchor_points_cached,
        )
        if len(control_panel_polygons_cached) != len(control_panel_instances_cached):
            control_panel_polygons_cached = []

    if isinstance(line_bay_info, dict):
        line_bay_info = deps.enrich_line_bay_reference_info(
            files,
            device_options,
            equip_map_sup,
            dict(line_bay_info),
        )

    for file_obj in files:
        file_name = get_file_name(file_obj)
        try:
            stem_norm = normalize_for_compare(Path(file_name).stem)
            if stem_norm in SKIP_BATCH_FILL_STEMS:
                out_path = _write_original_file(file_obj)
                _record_output(file_name, out_path)
                logs.append(f"{prefix_label}{file_name}: skipped fill (kept original geometry).")
                continue
            device_for_file = resolve_equipment_name(file_name, device_options, equip_map_sup)
            device_norm = normalize_for_compare(device_for_file)
            if device_norm and device_norm not in FORCED_CABIN_AUTO_CREATE_DEVICES:
                uploaded_device_norms.add(device_norm)
            if device_for_file not in instance_cache:
                instance_cache[device_for_file] = parse_supervisor_device_table(
                    sup_wb_path, sup_sheet, device_for_file
                )
            cached_instances = instance_cache.get(device_for_file, [])
            type_map_device = cached_instances[0].get("type_map", {}) if cached_instances else {}
            if not cached_instances:
                out_path = _write_original_file(file_obj)
                _record_output(file_name, out_path)
                logs.append(
                    f"{prefix_label}{file_name}: kept original file (no supervisor rows for device '{device_for_file or 'unknown'}')."
                )
                _validate_rewritten_output(device_for_file, file_name, out_path, cached_instances)
                continue
            inst = _pick_instance_for_file(file_name, cached_instances)
            seq_arg = None
            if len(cached_instances) > 1:
                seq_arg = cached_instances
            elif normalize_for_compare(device_for_file) in SEQUENTIAL_FILL_DEVICES:
                seq_arg = cached_instances
            inst_map = None
            default_fields = inst.get("fields") if inst else None
            if (
                cached_instances
                and ups_anchor_info is not None
                and normalize_for_compare(device_for_file) in PROTECTION_LAYOUT_DEVICES
            ):
                inst_map = {}
                for inst_item in cached_instances:
                    fields = inst_item.get("fields", {})
                    order = inst_item.get("order", [])
                    id_val = inst_item.get("id_value")
                    feeder_val = inst_item.get("feeder_value")
                    name_val = inst_item.get("name_value")
                    candidates = [id_val, name_val, feeder_val]
                    if pd.notna(id_val) and pd.notna(feeder_val):
                        candidates.append(f"{id_val}_{feeder_val}")
                        candidates.append(f"{feeder_val}_{id_val}")
                    for cand in candidates:
                        norm = normalize_value_for_compare(cand)
                        if norm and norm not in inst_map:
                            inst_map[norm] = (fields, order)
            out_path, used_layer = deps.fill_one_gpkg(
                file_obj,
                device_for_file,
                field_map=inst.get("fields") if inst else None,
                field_order=inst.get("order") if inst else None,
                instance_map=inst_map,
                default_fields=default_fields,
                sequential_instances=seq_arg,
                line_bay_info=line_bay_info,
                ups_anchor_info=ups_anchor_info,
                type_map=type_map_device,
                sup_wb_path=sup_wb_path,
                sup_sheet=sup_sheet,
                seq_assign_fallback=seq_assign_fallback,
                control_panel_polygons=control_panel_polygons_cached,
            )
            _record_output(file_name, out_path)
            log_instances = [inst] if inst else cached_instances
            run_domain_rows.extend(
                append_domain_code_log(
                    collect_domain_log_entries(log_instances),
                    {
                        "workbook": sup_wb_path.name if sup_wb_path else None,
                        "sheet": sup_sheet,
                        "device": device_for_file,
                        "output": f"{prefix_label}{file_name}",
                    },
                )
            )
            chosen_label = inst.get("label") if inst else "default instance"
            logs.append(
                f"{prefix_label}{file_name}: filled using device '{device_for_file}' ({chosen_label}) on layer '{used_layer}'."
            )
            _validate_rewritten_output(device_for_file, file_name, out_path, cached_instances)
        except Exception as exc:
            out_path = _write_original_file(file_obj)
            _record_output(file_name, out_path)
            logs.append(f"{prefix_label}{file_name}: failed ({exc}); kept original file.")
            _validate_rewritten_output(
                locals().get("device_for_file"),
                file_name,
                out_path,
                locals().get("cached_instances"),
            )

    protection_devices = [
        dev for dev in device_options if normalize_for_compare(dev) in PROTECTION_LAYOUT_DEVICES
    ]
    anchor = None
    anchor_crs = None
    spacing_val = PROTECTION_LAYOUT_SPACING
    if ups_anchor_info:
        anchor, anchor_crs = deps.load_ups_anchor_and_crs(
            ups_anchor_info.get("path"),
            ups_anchor_info.get("layer"),
        )
        try:
            spacing_val = float(ups_anchor_info.get("spacing", PROTECTION_LAYOUT_SPACING))
        except Exception:
            spacing_val = PROTECTION_LAYOUT_SPACING
    if protection_devices and not control_panel_polygons_cached and ups_anchor_info and anchor is None:
        logs.append(f"{prefix_label}Protection auto-create skipped: UPS anchor could not be resolved.")
    for dev_name in protection_devices:
        if normalize_for_compare(dev_name) in uploaded_device_norms:
            continue
        instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
        if not instances:
            continue
        points: list[Any] = []
        points_crs = anchor_crs
        if control_panel_polygons_cached:
            points = deps.build_points_in_panel_polygons(control_panel_polygons_cached, len(instances))
            if cabins_gdf_cached is not None:
                points_crs = cabins_gdf_cached.crs
        if (not points or len(points) != len(instances)) and anchor is not None:
            points = deps.build_protection_layout_points(anchor, len(instances), spacing_val)
            points_crs = anchor_crs
        if not points or len(points) != len(instances):
            logs.append(f"{prefix_label}{dev_name}: protection layout failed (no points).")
            continue
        out_gdf = build_device_gdf_from_instances(instances, points, points_crs)
        layer_name = derive_layer_name_from_filename(f"{dev_name}.gpkg")
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
            out_path = Path(tmpout.name)
        out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
        file_name = f"{layer_name}.gpkg"
        _record_output(file_name, out_path)
        run_domain_rows.extend(
            append_domain_code_log(
                collect_domain_log_entries(instances),
                {
                    "workbook": sup_wb_path.name if sup_wb_path else None,
                    "sheet": sup_sheet,
                    "device": dev_name,
                    "output": f"{prefix_label}{file_name}",
                },
            )
        )
        src_label = "control panels" if control_panel_polygons_cached else "UPS anchor"
        logs.append(f"{prefix_label}{dev_name}: auto-created protection points from {src_label} ({len(points)}).")

    cabin_aspatial_devices = [
        "Optical Telecommunication Equipment (Telecom)",
        "ODF",
    ]
    for dev_name in cabin_aspatial_devices:
        if normalize_for_compare(dev_name) in uploaded_device_norms:
            continue
        instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
        if not instances:
            logs.append(f"{prefix_label}{dev_name}: skipped (no instances in sheet).")
            continue
        out_df = build_device_table_from_instances(instances)
        if out_df.empty and len(out_df.columns) == 0:
            logs.append(f"{prefix_label}{dev_name}: skipped (no attributes to write).")
            continue
        layer_name = derive_layer_name_from_filename(dev_name)
        file_name = f"{dev_name}.gpkg"
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
            out_path = Path(tmpout.name)
        write_aspatial_gpkg_layer(out_df, out_path, layer_name)
        _record_output(file_name, out_path)
        run_domain_rows.extend(
            append_domain_code_log(
                collect_domain_log_entries(instances),
                {
                    "workbook": sup_wb_path.name if sup_wb_path else None,
                    "sheet": sup_sheet,
                    "device": dev_name,
                    "output": f"{prefix_label}{file_name}",
                },
            )
        )
        logs.append(f"{prefix_label}{dev_name}: auto-created as non-spatial table ({len(out_df)} record(s)).")

    cabin_point_devices = [
        "Distribution Transformer",
        "Standby Generator",
    ]
    cabin_polygon_devices = [
        "Control and Protection Panels",
        "Transformer Bay",
    ]
    cabin_spatial_devices = cabin_point_devices + cabin_polygon_devices
    if any(normalize_for_compare(device) not in uploaded_device_norms for device in cabin_spatial_devices):
        cabins_gdf = cabins_gdf_cached
        if cabins_gdf is None or cabins_gdf.empty:
            for dev_name in cabin_spatial_devices:
                if normalize_for_compare(dev_name) in uploaded_device_norms:
                    continue
                logs.append(f"{prefix_label}{dev_name}: skipped auto-create (no cabin polygons uploaded).")
        else:
            cabin_anchor_points = cabin_anchor_points_cached
            for dev_name in cabin_point_devices:
                if normalize_for_compare(dev_name) in uploaded_device_norms:
                    continue
                instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
                if not instances:
                    logs.append(f"{prefix_label}{dev_name}: skipped (no instances in sheet).")
                    continue
                points = deps.layout_points_in_cabins(cabins_gdf, len(instances), cabin_anchor_points)
                points = deps.expand_geometries(points, len(instances))
                if not points:
                    logs.append(f"{prefix_label}{dev_name}: cabin layout failed (no points).")
                    continue
                out_gdf = build_device_gdf_from_instances(instances, points, cabins_gdf.crs)
                layer_name = derive_layer_name_from_filename(dev_name)
                file_name = f"{dev_name}.gpkg"
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                    out_path = Path(tmpout.name)
                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                _record_output(file_name, out_path)
                run_domain_rows.extend(
                    append_domain_code_log(
                        collect_domain_log_entries(instances),
                        {
                            "workbook": sup_wb_path.name if sup_wb_path else None,
                            "sheet": sup_sheet,
                            "device": dev_name,
                            "output": f"{prefix_label}{file_name}",
                        },
                    )
                )
                logs.append(f"{prefix_label}{dev_name}: auto-created inside cabins ({len(points)} feature(s)).")

            for dev_name in cabin_polygon_devices:
                if normalize_for_compare(dev_name) in uploaded_device_norms:
                    continue
                instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
                if not instances:
                    logs.append(f"{prefix_label}{dev_name}: skipped (no instances in sheet).")
                    continue
                if normalize_for_compare(dev_name) == normalize_for_compare("Transformer Bay"):
                    polygons = deps.build_control_panel_polygons(
                        instances,
                        cabins_gdf,
                        cabin_anchor_points,
                        fixed_width_m=0.8,
                        fixed_depth_m=1.0,
                    )
                else:
                    polygons = deps.build_control_panel_polygons(instances, cabins_gdf, cabin_anchor_points)
                if not polygons or len(polygons) != len(instances):
                    logs.append(f"{prefix_label}{dev_name}: cabin polygon layout failed (no polygons).")
                    continue
                out_gdf = build_device_gdf_from_instances(instances, polygons, cabins_gdf.crs)
                layer_name = derive_layer_name_from_filename(dev_name)
                file_name = f"{dev_name}.gpkg"
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                    out_path = Path(tmpout.name)
                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                _record_output(file_name, out_path)
                run_domain_rows.extend(
                    append_domain_code_log(
                        collect_domain_log_entries(instances),
                        {
                            "workbook": sup_wb_path.name if sup_wb_path else None,
                            "sheet": sup_sheet,
                            "device": dev_name,
                            "output": f"{prefix_label}{file_name}",
                        },
                    )
                )
                logs.append(f"{prefix_label}{dev_name}: auto-created inside cabins ({len(polygons)} feature(s)).")

    template_devices = [
        ("High Voltage Line", HV_LINE_TEMPLATE_PATH),
        ("Earthing Transformer", EARTHING_TRANSFORMER_TEMPLATE_PATH),
    ]
    for dev_name, tpl_path in template_devices:
        dev_norm = normalize_for_compare(dev_name)
        expanded_instances: list[dict[str, Any]] = []
        expanded_geoms: list[Any] = []
        bay_gdf = None
        cabin_anchor_points: list[Any] = []
        if dev_norm in uploaded_device_norms:
            continue
        instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
        if not instances:
            logs.append(f"{prefix_label}{dev_name}: skipped (no instances in sheet).")
            continue
        if dev_norm == normalize_for_compare("High Voltage Line") and line_bay_info:
            bay_gdf = deps.load_line_bay_layer(
                line_bay_info.get("path"),
                line_bay_info.get("layer"),
                line_bay_info.get("field"),
            )
            if bay_gdf is not None and not bay_gdf.empty:
                bay_field = deps.pick_line_bay_name_field(bay_gdf, line_bay_info.get("field"))
                id_name_map = line_bay_info.get("id_name_map") if isinstance(line_bay_info, dict) else {}
                if not isinstance(id_name_map, dict):
                    id_name_map = {}
                geom_col = bay_gdf.geometry.name
                geoms_all = list(bay_gdf[geom_col])
                by_norm: dict[str, list[int]] = {}
                for idx, row in bay_gdf.iterrows():
                    name_val = deps.extract_bay_name_from_row(row, bay_field, id_name_map)
                    norm = normalize_value_for_compare(name_val)
                    if not norm:
                        continue
                    by_norm.setdefault(norm, []).append(idx)
                unused_ids = list(range(len(bay_gdf)))
                unused_set = set(unused_ids)

                def _take_unused() -> int | None:
                    while unused_ids:
                        idx = unused_ids.pop(0)
                        if idx in unused_set:
                            unused_set.remove(idx)
                            return idx
                    return None

                lightning_norms = {normalize_for_compare("Lightning Arrester")}
                preferred_points = deps.collect_device_points_from_uploads(
                    files, bay_gdf.crs, device_options, equip_map_sup, lightning_norms
                )
                all_points = deps.collect_point_geometries_from_uploads(files, bay_gdf.crs)
                points_source = (
                    preferred_points
                    if preferred_points is not None and not preferred_points.empty
                    else all_points
                )
                points_by_bay = deps.map_points_to_bays(points_source, bay_gdf) if points_source is not None else {}
                for inst in instances:
                    inst_fields = inst.get("fields", {}) or {}
                    candidates = [inst.get("id_value"), inst.get("name_value"), inst.get("feeder_value")]
                    for key, val in inst_fields.items():
                        norm_key = normalize_for_compare(key)
                        if any(token in norm_key for token in ["linebay", "line_bay", "bayname", "name"]):
                            candidates.append(val)
                    chosen_idx = None
                    for cand in candidates:
                        cand_norms: list[str] = []
                        norm = normalize_value_for_compare(cand)
                        if norm:
                            cand_norms.append(norm)
                            stripped = norm.rstrip("0123456789").rstrip()
                            if stripped and stripped not in cand_norms:
                                cand_norms.append(stripped)
                        for candidate_norm in cand_norms:
                            if candidate_norm and candidate_norm in by_norm and by_norm[candidate_norm]:
                                chosen_idx = by_norm[candidate_norm].pop(0)
                                if chosen_idx in unused_set:
                                    unused_set.remove(chosen_idx)
                                break
                        if chosen_idx is not None:
                            break
                    if chosen_idx is None:
                        chosen_idx = _take_unused()
                    if chosen_idx is None and geoms_all:
                        chosen_idx = 0
                    if chosen_idx is None:
                        continue
                    poly = geoms_all[chosen_idx]
                    try:
                        bay_row = bay_gdf.iloc[chosen_idx]
                        bay_name_value = deps.extract_bay_name_from_row(bay_row, bay_field, id_name_map)
                    except Exception:
                        bay_name_value = None
                    points_in_bay = points_by_bay.get(chosen_idx, [])
                    lines = deps.build_lines_from_points_in_polygon(poly, points_in_bay, 3)
                    lines = deps.expand_geometries(lines, 3)
                    if not lines:
                        continue
                    for line in lines:
                        inst_copy = dict(inst)
                        fields_copy = dict(inst.get("fields", {}) or {})
                        if bay_name_value is not None:
                            for name_col in [
                                "Name",
                                "name",
                                "Line_Name",
                                "line_name",
                                "line",
                                "Line",
                                "Line_Bay_Name",
                                "line_bay_name",
                            ]:
                                fields_copy[name_col] = bay_name_value
                        inst_copy["fields"] = fields_copy
                        expanded_instances.append(inst_copy)
                        expanded_geoms.append(line)
            if expanded_geoms and bay_gdf is not None:
                out_gdf = build_device_gdf_from_instances(expanded_instances, expanded_geoms, bay_gdf.crs)
                out_gdf = deps.ensure_name_fields_string(
                    out_gdf,
                    [
                        "Name",
                        "Line_Name",
                        "Line_Bay_Name",
                        "line_name",
                        "line_bay_name",
                        "line",
                        "Line",
                    ],
                )
                layer_name = derive_layer_name_from_filename(dev_name)
                file_name = f"{dev_name}.gpkg"
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                    out_path = Path(tmpout.name)
                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                _record_output(file_name, out_path)
                run_domain_rows.extend(
                    append_domain_code_log(
                        collect_domain_log_entries(instances),
                        {
                            "workbook": sup_wb_path.name if sup_wb_path else None,
                            "sheet": sup_sheet,
                            "device": dev_name,
                            "output": f"{prefix_label}{file_name}",
                        },
                    )
                )
                logs.append(
                    f"{prefix_label}{dev_name}: auto-created from Line Bay polygons ({len(expanded_geoms)} feature(s))."
                )
                continue
        if dev_norm == normalize_for_compare("Earthing Transformer"):
            cabin_norms = {normalize_for_compare("Substation/Cabin")}
            cabins_gdf = deps.collect_device_polygons_from_uploads(
                files, None, device_options, equip_map_sup, cabin_norms
            )
            cabin_anchor_points = deps.build_cabin_anchor_points(
                files,
                cabins_gdf,
                device_options,
                equip_map_sup,
            )
            geoms = [pt for pt in cabin_anchor_points if pt is not None]
            if geoms:
                target_count = len(instances)
                geoms = deps.expand_geometries(geoms, target_count)
                out_gdf = build_device_gdf_from_instances(
                    instances, geoms, cabins_gdf.crs if cabins_gdf is not None else None
                )
                try:
                    out_gdf = out_gdf.copy()
                    out_gdf.geometry = out_gdf.geometry.centroid
                except Exception:
                    pass
                layer_name = derive_layer_name_from_filename(dev_name)
                file_name = f"{dev_name}.gpkg"
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                    out_path = Path(tmpout.name)
                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                _record_output(file_name, out_path)
                run_domain_rows.extend(
                    append_domain_code_log(
                        collect_domain_log_entries(instances),
                        {
                            "workbook": sup_wb_path.name if sup_wb_path else None,
                            "sheet": sup_sheet,
                            "device": dev_name,
                            "output": f"{prefix_label}{file_name}",
                        },
                    )
                )
                logs.append(
                    f"{prefix_label}{dev_name}: auto-created beside switchgear inside cabins ({len(geoms)} feature(s))."
                )
                continue
            logs.append(f"{prefix_label}{dev_name}: skipped auto-create (no cabin polygons uploaded).")
            continue
        tpl = deps.load_template_layer(tpl_path)
        if tpl is None:
            logs.append(f"{prefix_label}{dev_name}: template not found at {tpl_path}.")
            continue
        tpl_gdf, _tpl_layer = tpl
        geoms = list(tpl_gdf.geometry)
        if dev_norm == normalize_for_compare("Earthing Transformer") and cabin_anchor_points:
            geoms = cabin_anchor_points.copy()
        if dev_norm == normalize_for_compare("Earthing Transformer"):
            clean_geoms: list[Any] = []
            for geom in geoms:
                if geom is None or getattr(geom, "is_empty", True):
                    continue
                if getattr(geom, "geom_type", "").lower() == "point":
                    clean_geoms.append(geom)
                else:
                    try:
                        clean_geoms.append(geom.centroid)
                    except Exception:
                        continue
            geoms = clean_geoms
        if dev_norm == normalize_for_compare("High Voltage Line"):
            instances = repeat_instances(instances, 3)
        target_count = len(instances)
        if target_count <= 0:
            logs.append(f"{prefix_label}{dev_name}: skipped (no instances to fill).")
            continue
        geoms = deps.expand_geometries(geoms, target_count)
        if not geoms:
            logs.append(f"{prefix_label}{dev_name}: template has no geometry.")
            continue
        out_gdf = build_device_gdf_from_instances(instances, geoms, tpl_gdf.crs)
        if dev_norm == normalize_for_compare("Earthing Transformer"):
            try:
                out_gdf = out_gdf.copy()
                out_gdf.geometry = out_gdf.geometry.centroid
            except Exception:
                pass
        if dev_norm == normalize_for_compare("High Voltage Line"):
            id_name_map = line_bay_info.get("id_name_map") if isinstance(line_bay_info, dict) else {}
            if isinstance(id_name_map, dict) and id_name_map:
                out_gdf = deps.replace_line_name_ids(out_gdf, id_name_map)
            out_gdf = deps.ensure_name_fields_string(
                out_gdf,
                [
                    "Name",
                    "Line_Name",
                    "Line_Bay_Name",
                    "line_name",
                    "line_bay_name",
                    "line",
                    "Line",
                ],
            )
        layer_name = derive_layer_name_from_filename(dev_name)
        file_name = f"{dev_name}.gpkg"
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
            out_path = Path(tmpout.name)
        out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
        _record_output(file_name, out_path)
        run_domain_rows.extend(
            append_domain_code_log(
                collect_domain_log_entries(instances),
                {
                    "workbook": sup_wb_path.name if sup_wb_path else None,
                    "sheet": sup_sheet,
                    "device": dev_name,
                    "output": f"{prefix_label}{file_name}",
                },
            )
        )
        logs.append(f"{prefix_label}{dev_name}: auto-created from template ({len(geoms)} feature(s)).")

    for spec in rewritten_id_validation_specs():
        dev_name = spec.get("device_name")
        dev_norm = normalize_for_compare(dev_name)
        if not dev_norm or dev_norm in validated_rewrite_devices:
            continue
        if dev_name not in instance_cache:
            instance_cache[dev_name] = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
        instances = instance_cache.get(dev_name, [])
        workbook_ids = dedupe_id_texts(
            [inst.get("id_value") for inst in instances if isinstance(inst, dict)]
        )
        if not workbook_ids:
            continue
        _validate_rewritten_output(dev_name, None, None, instances)

    return outputs, logs, run_domain_rows
