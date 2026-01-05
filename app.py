import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
import math
import unicodedata
import statistics
import difflib
import re
import json
from concurrent.futures import ProcessPoolExecutor

import geopandas as gpd
import pandas as pd
import streamlit as st

# =====================================================================
# PATHS
# =====================================================================
BASE_DIR = Path(__file__).parent
REFERENCE_DATA_DIR = BASE_DIR / "reference_data"
SUPERVISOR_WORKBOOK_DIR = BASE_DIR / "supervisor_workbooks"
# Preferred workbook order: newest first; falls back to any available in reference_data.
WORKBOOK_PRIORITY = [
    "SUBSTATION 1-25102025.xlsx",
    "SUBSTATIONS 2-25112025.xlsx",
    "SUBSTATIONS 2-251025.xlsx",
]
WORKBOOK_NAME = WORKBOOK_PRIORITY[0]
WORKBOOK_PATH = REFERENCE_DATA_DIR / WORKBOOK_NAME
REFERENCE_EXTENSIONS = (".xlsx", ".xlsm")
ALIAS_FILE = REFERENCE_DATA_DIR / "alias_map.json"
GPKG_EQUIP_MAP_FILE = REFERENCE_DATA_DIR / "gpkg_equipment_map.json"
MAPPING_CACHE_FILE = REFERENCE_DATA_DIR / "schema_mapping_cache.json"
TEMPLATE_DIR = BASE_DIR / "For High Voltage Line"
HV_LINE_TEMPLATE_PATH = TEMPLATE_DIR / "High Voltage Lines.gpkg"
EARTHING_TRANSFORMER_TEMPLATE_PATH = TEMPLATE_DIR / "EARTHING TRANSFORMER.gpkg"

PREVIEW_ROWS = 30
MAX_GPKG_NAME_LENGTH = 254

# Curated equipment names from the "Electric device" schema sheet (hard-coded for stability/order).
ELECTRIC_DEVICE_EQUIPMENT = [
    "Power Transformer/ Stepup Transformer",
    "Earthing Transformer",
    "High Voltage Busbar/Medium Voltage Busbar",
    "MV Switch gear",
    "Line Bay",
    "Voltage Transformer",
    "Current Transformer",
    "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
    "High Voltage Switch/High Voltage Switch",
    "Uninterruptable power supply(UPS)",
    "Substation/Cabin",
    "Lightning Arrester",
    "DC Supply 48 VDC Battery",
    "DC Supply 110 VDC Battery",
    "DC Supply 48 VDC charger",
    "DC Supply 110 VDC charger",
    "DIGITAL fault recorder",
    "High Voltage Line",
    "Transformer Bay",
    "Indoor Circuit Breaker/30kv/15kb",
    "Indoor Current Transformer",
    "Indoor Voltage Transformer",
    "Control and Protection Panels",
    "Distance Protection",
    "Transformer Protection",
    "Line Overcurrent Protection",
    "Standby Generator",
]

# =====================================================================
# HEADER CLEANING UTILITIES
# =====================================================================

INVISIBLE_HEADER_CHARS = ["\ufeff", "\u200b", "\u200c", "\u200d", "\xa0"]
COMPARISON_IGNORED_CHARS = " -_,./()\\"
COMPARISON_TRANSLATION_TABLE = str.maketrans("", "", COMPARISON_IGNORED_CHARS)


def strip_unicode_spaces(text: str) -> str:
    """Remove ALL Unicode whitespace including NBSP, thin space, etc."""
    if not isinstance(text, str):
        return text
    return "".join(ch for ch in text if unicodedata.category(ch) != "Zs")


def _clean_column_name(name: Any) -> str:
    """Clean column names (remove NBSP, collapse spaces, keep punctuation)."""
    text = "" if name is None else str(name)

    # Normalize Unicode whitespace: convert non-breaking/thin spaces to regular space, keep ASCII spaces
    text = "".join(" " if unicodedata.category(ch) == "Zs" else ch for ch in text)

    # Remove invisible BOM-type chars
    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")

    # Normalize: collapse multiple spaces
    text = " ".join(text.split())

    return text.strip()


def ensure_unique_columns(columns: list[str]) -> list[str]:
    """
    Make column names unique by appending suffixes for duplicates.
    Example: ['A', 'A'] -> ['A', 'A_2']
    """
    seen: dict[str, int] = {}
    unique: list[str] = []
    for col in columns:
        base = col or ""
        count = seen.get(base, 0) + 1
        seen[base] = count
        unique.append(base if count == 1 else f"{base}_{count}")
    return unique


@st.cache_data(show_spinner=False)
def list_reference_workbooks() -> dict[str, Path]:
    """Return mapping of display label -> workbook path for supported extensions."""
    workbooks = {}
    if REFERENCE_DATA_DIR.exists():
        for p in sorted(REFERENCE_DATA_DIR.glob("**/*")):
            if p.is_file() and p.suffix.lower() in REFERENCE_EXTENSIONS:
                label = p.relative_to(REFERENCE_DATA_DIR).as_posix()
                workbooks[label] = p
    return workbooks


@st.cache_data(show_spinner=False)
def list_supervisor_workbooks() -> dict[str, Path]:
    """Return mapping of display label -> supervisor workbook path."""
    workbooks = {}
    if SUPERVISOR_WORKBOOK_DIR.exists():
        for p in sorted(SUPERVISOR_WORKBOOK_DIR.glob("**/*")):
            if p.is_file() and p.suffix.lower() in REFERENCE_EXTENSIONS:
                label = p.relative_to(SUPERVISOR_WORKBOOK_DIR).as_posix()
                workbooks[label] = p
    return workbooks


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


def detect_equipment_type_column(df: pd.DataFrame) -> str | None:
    """Heuristic to pick a column describing equipment type/name."""
    if df.empty:
        return None
    candidates = []
    keywords = ["type", "equipment", "asset", "class", "category", "device", "description", "name"]
    for col in df.columns:
        norm = normalize_for_compare(col)
        score = sum(1 for kw in keywords if kw in norm)
        if score:
            candidates.append((score, len(norm), col))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


def to_metric(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Project to a metric CRS for distance if needed."""
    if gdf.crs is None:
        return gdf
    if gdf.crs.is_geographic:
        try:
            return gdf.to_crs(3857)
        except Exception:
            return gdf
    return gdf


@st.cache_data(show_spinner=False)
def list_gpkg_layers(path: Path) -> list[str]:
    """List layers inside a GeoPackage path."""
    try:
        import pyogrio

        info = pyogrio.list_layers(path)
        if hasattr(info, "name"):
            return list(info["name"])
        return [row[0] for row in info] if info else []
    except Exception:
        try:
            import fiona

            return fiona.listlayers(path)
        except Exception:
            return []


_REFERENCE_ALIAS_COLUMNS: list[str] | None = None
_FILE_ALIAS_CACHE: dict[str, list[str]] | None = None
_GPKG_EQUIP_MAP: dict[str, str] | None = None
_MAPPING_CACHE: dict[str, dict[str, str]] | None = None
_EXCEL_FILE_CACHE: dict[str, pd.ExcelFile] = {}
_SHEET_HEADER_CACHE: dict[tuple[str, str], list[str]] = {}
_REFERENCE_SHEET_CACHE: dict[tuple[str, str], pd.DataFrame] = {}
_SUB_COL_CACHE: dict[tuple[str, str], str | None] = {}
_DOMAIN_CODE_LOOKUP: dict[str, Any] | None = None


def get_reference_columns() -> list[str]:
    """Collect column names from reference GeoPackages to enrich fuzzy aliases."""
    global _REFERENCE_ALIAS_COLUMNS
    if _REFERENCE_ALIAS_COLUMNS is not None:
        return _REFERENCE_ALIAS_COLUMNS
    cols: set[str] = set()
    try:
        for p in REFERENCE_DATA_DIR.glob("*.gpkg"):
            for lyr in list_gpkg_layers(p):
                try:
                    gdf = gpd.read_file(p, layer=lyr, rows=1)
                    cols.update(gdf.columns)
                except Exception:
                    continue
    except Exception:
        pass
    _REFERENCE_ALIAS_COLUMNS = list(cols)
    return _REFERENCE_ALIAS_COLUMNS


def load_file_aliases() -> dict[str, list[str]]:
    """Load persisted aliases from reference_data/alias_map.json if present."""
    global _FILE_ALIAS_CACHE
    if _FILE_ALIAS_CACHE is not None:
        return _FILE_ALIAS_CACHE
    if ALIAS_FILE.exists():
        try:
            data = json.loads(ALIAS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _FILE_ALIAS_CACHE = {k: v if isinstance(v, list) else [] for k, v in data.items()}
                return _FILE_ALIAS_CACHE
        except Exception:
            pass
    _FILE_ALIAS_CACHE = {}
    return _FILE_ALIAS_CACHE


def load_gpkg_equipment_map() -> dict[str, str]:
    """Load gpkg->equipment mapping from reference_data/gpkg_equipment_map.json, with defaults."""
    global _GPKG_EQUIP_MAP
    if _GPKG_EQUIP_MAP is not None:
        return _GPKG_EQUIP_MAP
    default_map = {
        "110vdc battery": "DC Supply 110 VDC Battery",
        "110vdc charger": "DC Supply 110 VDC charger",
        "48vdc battery": "DC Supply 48 VDC Battery",
        "48vdc charger": "DC Supply 48 VDC charger",
        "busbar": "High Voltage Busbar/Medium Voltage Busbar",
        "cabin": "Substation/Cabin",
        "cb indor switchgear": "Indoor Circuit Breaker/30kv/15kb",
        "ct indor switchgear": "Indoor Current Transformer",
        "current transformer": "Current Transformer",
        "digital fault recorder": "DIGITAL fault recorder",
        "disconnector switch": "High Voltage Switch/High Voltage Switch",
        "high voltage circuit breaker": "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
        "indor switchgear table": "MV Switch gear",
        "lightning arrestor": "Lightning Arrester",
        "line bay": "Line Bay",
        "power cable to transformer": "Transformer Bay",
        "transformers": "Transformer Bay",
        "voltage transformer": "Voltage Transformer",
        "vt indor switchgear": "Indoor Voltage Transformer",
        "ups": "Uninterruptable power supply(UPS)",
        "trans_system prot1": "Distance Protection",
        "telecom": "Control and Protection Panels",
        # Additional aliases from provided mapping
        "high_voltage_circuit_breaker": "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
        "high_voltage_circuit_breaker_high_voltage_circuit_breaker": "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
        "line": "Line Bay",
        "linebay": "Line Bay",
        "line_bay": "Line Bay",
        "voltage_transformer": "Voltage Transformer",
        "current_transformer": "Current Transformer",
        "indoor_current_transformer": "Indoor Current Transformer",
        "indoor_voltage_transformer": "Indoor Voltage Transformer",
        "indoorcircuitbreaker": "Indoor Circuit Breaker/30kv/15kb",
        "telecom_sdh": "Control and Protection Panels",
        "telecom_odf": "Control and Protection Panels",
        "highvoltage_line": "Line Bay",
        "transformer_bay": "Transformer Bay",
        "power_transformer": "Power Transformer/ Stepup Transformer",
        "powertransformer": "Power Transformer/ Stepup Transformer",
        "telecom": "Control and Protection Panels",
        "telecom_odf": "Control and Protection Panels",
    }
    if GPKG_EQUIP_MAP_FILE.exists():
        try:
            data = json.loads(GPKG_EQUIP_MAP_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # normalize keys
                loaded = {normalize_for_compare(k): str(v) for k, v in data.items()}
                default_map.update(loaded)
        except Exception:
            pass
    # Canonicalize mapped values to closest equipment option (if available)
    canon_map: dict[str, str] = {}
    try:
        import difflib
    except Exception:
        difflib = None  # type: ignore
    for norm_key, val in default_map.items():
        target = val
        try:
            if difflib:
                best = difflib.get_close_matches(
                    normalize_for_compare(val), [normalize_for_compare(e) for e in ELECTRIC_DEVICE_EQUIPMENT], n=1, cutoff=0.5
                )
                if best:
                    match_norm = best[0]
                    for opt in ELECTRIC_DEVICE_EQUIPMENT:
                        if normalize_for_compare(opt) == match_norm:
                            target = opt
                            break
        except Exception:
            target = val
        canon_map[norm_key] = target
    _GPKG_EQUIP_MAP = canon_map
    return _GPKG_EQUIP_MAP


def load_mapping_cache() -> dict[str, dict[str, str]]:
    """Load persisted field mapping choices keyed by schema/sheet/equipment."""
    global _MAPPING_CACHE
    if _MAPPING_CACHE is not None:
        return _MAPPING_CACHE
    if MAPPING_CACHE_FILE.exists():
        try:
            data = json.loads(MAPPING_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _MAPPING_CACHE = {str(k): v if isinstance(v, dict) else {} for k, v in data.items()}
                return _MAPPING_CACHE
        except Exception:
            pass
    _MAPPING_CACHE = {}
    return _MAPPING_CACHE


def save_mapping_cache(cache: dict[str, dict[str, str]]) -> None:
    try:
        MAPPING_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception:
        pass


def resolve_equipment_name(file_name: str, equipment_options: list[str], equip_map: dict[str, str]) -> str:
    """Pick equipment/device name for a given file using explicit map then similarity."""
    norm_file = normalize_for_compare(Path(file_name).stem)
    override = FILE_DEVICE_OVERRIDES.get(norm_file)
    if override:
        if override in equipment_options:
            return override
        try:
            import difflib

            best = difflib.get_close_matches(
                normalize_for_compare(override),
                [normalize_for_compare(e) for e in equipment_options],
                n=1,
                cutoff=0.8,
            )
            if best:
                match_norm = best[0]
                for opt in equipment_options:
                    if normalize_for_compare(opt) == match_norm:
                        return opt
        except Exception:
            pass
    mapped = equip_map.get(norm_file)
    if mapped:
        if mapped in equipment_options:
            return mapped
        try:
            import difflib

            best = difflib.get_close_matches(
                normalize_for_compare(mapped),
                [normalize_for_compare(e) for e in equipment_options],
                n=1,
                cutoff=0.8,
            )
            if best:
                match_norm = best[0]
                for opt in equipment_options:
                    if normalize_for_compare(opt) == match_norm:
                        return opt
        except Exception:
            pass
    try:
        import difflib

        best = difflib.get_close_matches(norm_file, [normalize_for_compare(e) for e in equipment_options], n=1, cutoff=0.5)
        if best:
            match_norm = best[0]
            for opt in equipment_options:
                if normalize_for_compare(opt) == match_norm:
                    return opt
    except Exception:
        pass
    return equipment_options[0] if equipment_options else ""


def parse_supervisor_device_table(workbook_path: Path, sheet_name: str, device_name: str) -> list[dict[str, Any]]:
    """
    Parse a supervisor-provided Electric device sheet where columns are:
    col0=device, col1=field, col2=type, value in rightmost non-null cell of the row.
    Supports multiple instances (e.g., multiple Line Bays) by returning a list of
    dicts with metadata: {"label": str, "fields": {field: value}, "id_value": Any, "name_value": Any}.
    """
    raw = pd.read_excel(workbook_path, sheet_name=sheet_name, dtype=str, header=None)

    target_norm = normalize_for_compare(device_name)
    is_protection = target_norm in PROTECTION_LAYOUT_DEVICES
    domain_code_map: dict[str, Any] = {}
    if raw.shape[1] > 4:
        for _, row in raw.iterrows():
            dom_val = row.iloc[3]
            code_val = row.iloc[4]
            if pd.isna(dom_val) or pd.isna(code_val):
                continue
            dom_norm = normalize_value_for_compare(dom_val)
            if dom_norm and dom_norm not in domain_code_map:
                domain_code_map[dom_norm] = code_val
    global_domain_map = load_domain_code_lookup()
    for key, val in global_domain_map.items():
        if key not in domain_code_map:
            domain_code_map[key] = val
    instances: list[dict[str, Any]] = []
    current_fields: dict[str, Any] | None = None

    def _extract_value(row: pd.Series, dtype: str) -> Any:
        def _is_blank(value: Any) -> bool:
            try:
                if pd.isna(value):
                    return True
            except Exception:
                pass
            if value is None:
                return True
            if isinstance(value, str):
                text = value.strip()
                if text == "":
                    return True
                if text.lower() == "not existing":
                    return True
            return False

        def _looks_like_unit_value(value: Any) -> bool:
            if value is None:
                return False
            text = str(value)
            if text.strip() == "":
                return False
            has_digit = any(ch.isdigit() for ch in text)
            has_alpha = any(ch.isalpha() for ch in text)
            return has_digit and has_alpha

        val = row.iloc[3] if len(row) > 3 else pd.NA
        domain_code = row.iloc[4] if len(row) > 4 else pd.NA

        norm_type = normalize_for_compare(dtype or "")
        is_numeric = any(tok in norm_type for tok in ("int", "integer", "long", "short", "bigint", "smallint", "double", "float", "decimal", "real", "number"))

        if is_numeric and not _is_blank(domain_code):
            return domain_code
        if is_numeric and not _is_blank(val):
            if _looks_like_unit_value(val) and _is_blank(domain_code):
                return val
            dom_norm = normalize_value_for_compare(val)
            mapped = domain_code_map.get(dom_norm)
            if mapped is not None and not _is_blank(mapped):
                return mapped
        if not _is_blank(val):
            return val

        if len(row) > 3:
            for v in row.iloc[3:]:
                if not _is_blank(v):
                    return v
        return pd.NA

    def _get_by_alias(fields: dict[str, Any], aliases: list[str]) -> Any:
        lookup = {normalize_for_compare(k): k for k in fields}
        for alias in aliases:
            key = lookup.get(normalize_for_compare(alias))
            if key is not None:
                return fields.get(key)
        return None

    def _finalize_instance(fields: dict[str, Any], order: list[str]) -> None:
        if not fields:
            return
        idx = len(instances) + 1
        id_value = _get_by_alias(
            fields,
            [
                "linebayid",
                "line_bay_id",
                "bayid",
                "deviceid",
                "id",
                "bay_meter_serial_number",
                "voltagetransformer_id",
                "voltagetransfomer_id",
                "voltage transformer id",
                "transfomerid",
                "transfomer id",
                "transformer_id",
                "currenttransformer_id",
                "current transformer id",
                "currenttransformerid",
                "current transfomer id",
                "switchgearid",
                "switchgear_id",
                "mv_switchgear_id",
                "mv switch gear id",
                "arresterid",
                "lightningarresterid",
                "lightiningarresterid",
                "hv_switch_id",
                "hvswitchid",
                "composite_id",
            ],
        )
        name_value = _get_by_alias(
            fields,
            [
                "linebayname",
                "line_bay_name",
                "bayname",
                "name",
                "voltagetransformer_name",
                "transformer_name",
                "voltagetransfomer_name",
                "voltage transformer name",
                "currenttransformer_name",
                "current transformer name",
                "current transfomer name",
                "circuit breaker name",
                "circuitbreakername",
                "switchgearname",
                "switchgear_name",
                "arrestername",
                "lightningarrestername",
                "lightiningarrestername",
            ],
        )
        feeder_value = _get_by_alias(fields, ["feederid", "feeder_id", "feeder", "feeder name", "feedername"])

        label_parts = [device_name]
        extra_parts = []
        if pd.notna(id_value):
            extra_parts.append(str(id_value))
        if pd.notna(feeder_value):
            extra_parts.append(f"Feeder {feeder_value}")
        if pd.notna(name_value) and normalize_for_compare(name_value) != normalize_for_compare(id_value):
            extra_parts.append(str(name_value))
        if not extra_parts:
            extra_parts.append(f"#{idx}")
        label = f"{device_name} - {', '.join(extra_parts)}"
        instances.append(
            {
                "label": label,
                "fields": fields,
                "id_value": id_value,
                "name_value": name_value,
                "feeder_value": feeder_value,
                "order": order.copy(),
            }
        )

    current_order: list[str] = []

    def _get_protection_type_cache() -> dict[str, str]:
        try:
            cache = st.session_state.get("protection_type_cache")
            if not isinstance(cache, dict):
                cache = {}
                st.session_state["protection_type_cache"] = cache
            return cache
        except Exception:
            return {}

    for _, row in raw.iterrows():
        dev_cell = row.iloc[0]
        dev_norm = normalize_for_compare(dev_cell) if pd.notna(dev_cell) else ""
        row_blank = row.iloc[1:].isna().all()

        if dev_norm == target_norm:
            if current_fields is not None and current_fields:
                _finalize_instance(current_fields, current_order)
            current_fields = {}
            current_order = []
        elif pd.notna(dev_cell):
            if current_fields is not None and current_fields:
                _finalize_instance(current_fields, current_order)
            current_fields = None
            current_order = []

        if current_fields is None:
            continue

        if row_blank:
            # Allow sparse blank rows inside a device block; finalize only when we see a new device or end of sheet.
            continue

        field = row.iloc[1]
        if pd.isna(field):
            continue
        field_clean = _clean_column_name(field)
        type_str = row.iloc[2] if len(row) > 2 else ""
        if pd.isna(type_str):
            type_str = ""
        if not isinstance(type_str, str):
            type_str = str(type_str)
        type_str = type_str.strip()
        if is_protection:
            cache = _get_protection_type_cache()
            cache_key = normalize_for_compare(field_clean)
            if type_str:
                cache[cache_key] = type_str
            else:
                cached = cache.get(cache_key)
                if cached:
                    type_str = cached
                else:
                    type_str = "Double"
                    cache[cache_key] = type_str
        val = _extract_value(row, type_str)
        series_val = pd.Series([val])
        coerced = coerce_series_to_type(series_val, type_str).iloc[0]
        current_fields[field_clean] = coerced
        if field_clean not in current_order:
            current_order.append(field_clean)

    if current_fields is not None and current_fields:
        _finalize_instance(current_fields, current_order)

    return instances


def process_single_gpkg(args):
    (
        gpkg,
        equipment_options_auto,
        equip_map,
        schema_path_auto,
        schema_sheet_auto,
        mapping_threshold_auto,
        keep_unmatched_auto,
        accept_threshold,
        tmp_out_str,
    ) = args
    try:
        gpkg = Path(gpkg)
        layers = list_gpkg_layers(gpkg)
        if not layers:
            return None, f"{gpkg.name}: no layers found."
        equipment_name = resolve_equipment_name(gpkg.name, equipment_options_auto, equip_map)
        schema_fields_auto, type_map_auto = load_schema_fields(schema_path_auto, schema_sheet_auto, equipment_name)
        out_path = Path(tmp_out_str) / gpkg.name
        if out_path.exists():
            out_path.unlink()
        for lyr in layers:
            gdf_layer = gpd.read_file(gpkg, layer=lyr)
            layer_name_out = derive_layer_name_from_filename(lyr)
            exclude_cols = {gdf_layer.geometry.name} if hasattr(gdf_layer, "geometry") else set()
            suggested, score_map = fuzzy_map_columns_with_scores(
                list(gdf_layer.columns), schema_fields_auto, threshold=mapping_threshold_auto, exclude=exclude_cols
            )
            norm_col_lookup = {normalize_for_compare(c): c for c in gdf_layer.columns}
            n = len(gdf_layer)

            def _na_series():
                return pd.Series([pd.NA] * n, index=gdf_layer.index)

            out_cols = {}
            for f in schema_fields_auto:
                src = suggested.get(f)
                chosen_src = None
                if src:
                    resolved = norm_col_lookup.get(normalize_for_compare(src), src)
                    if resolved in gdf_layer.columns:
                        chosen_src = resolved
                out_cols[f] = gdf_layer[chosen_src] if chosen_src else _na_series()
            if keep_unmatched_auto:
                for col in gdf_layer.columns:
                    if col not in suggested.values() and (not hasattr(gdf_layer, "geometry") or col != gdf_layer.geometry.name):
                        out_cols[f"orig_{col}"] = gdf_layer[col]
            geom_series = gdf_layer.geometry if hasattr(gdf_layer, "geometry") else None
            for f in schema_fields_auto:
                out_cols[f] = coerce_series_to_type(out_cols[f], type_map_auto.get(f, ""))
            out_layer = gpd.GeoDataFrame(out_cols, geometry=geom_series, crs=gdf_layer.crs)
            out_layer = sanitize_gdf_for_gpkg(out_layer)
            out_layer.to_file(out_path, driver="GPKG", layer=layer_name_out)
        return out_path, f"{gpkg.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'."
    except Exception as exc:
        return None, f"{Path(gpkg).name}: failed ({exc})."


def _cache_key_from_path(path: Path | str) -> str:
    """Stable string key for caching by filesystem path."""
    try:
        return str(Path(path).resolve())
    except Exception:
        return str(path)


def _excel_key_from_file(excel_file: pd.ExcelFile) -> str:
    if hasattr(excel_file, "_cache_key"):
        return getattr(excel_file, "_cache_key")
    try:
        return _cache_key_from_path(getattr(excel_file, "io", excel_file))
    except Exception:
        return str(excel_file)


def get_excel_file(workbook_path: Path) -> pd.ExcelFile:
    """Return cached pd.ExcelFile for a workbook path."""
    key = _cache_key_from_path(workbook_path)
    cached = _EXCEL_FILE_CACHE.get(key)
    if cached is not None:
        return cached
    excel_file = pd.ExcelFile(workbook_path)
    setattr(excel_file, "_cache_key", key)
    _EXCEL_FILE_CACHE[key] = excel_file
    return excel_file


def _get_sheet_header(excel_file: pd.ExcelFile, sheet: str) -> list[str] | None:
    """Return cleaned header for a sheet (cached, minimal rows read)."""
    key = (_excel_key_from_file(excel_file), sheet)
    if key in _SHEET_HEADER_CACHE:
        return _SHEET_HEADER_CACHE[key]
    try:
        raw_df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str, header=None, nrows=15)
        header_row = _detect_header_row(raw_df)
        header = ensure_unique_columns([_clean_column_name(c) for c in raw_df.iloc[header_row]])
        _SHEET_HEADER_CACHE[key] = header
        return header
    except Exception:
        return None


def fuzzy_map_columns(
    source_cols: list[str], target_fields: list[str], threshold: float = 0.6, exclude: set[str] | None = None
) -> dict[str, str]:
    """Return mapping target_field -> source_col using rich fuzzy/alias logic."""
    exclude = exclude or set()
    alias_map = {
        "countryofmanufacturer": ["manufacturingcountry", "countryofmanufacturing", "countryoforigin", "countryofmanufacture"],
        "countryofmanufacture": ["countryofmanufacturer", "countrymanufacturer"],
        "manufacturer": ["manufactoringcompany", "manufacturingcompany"],
        "manufactureryear": ["manufacturingyear", "yearofmanufacturer", "manufacturing_year"],
        "temperature range": ["temperaturerange", "temperature_range"],
        "typemodel": ["type_model", "type/model", "type model", "type-model"],
        "standards": ["standard", "std"],
        "standard": ["standards", "std"],
        "light_impulse_withsand_kv": [
            "impulsewithstandvoltage",
            "impulsewithstand",
            "impulsewithstandvoltage1250msfullwavekv",
            "impulsewithstandvoltage1250msfullwave",
            "impulsewithstandvoltagepeak",
        ],
        "ratedimpulsewithstandvol": [
            "impulsewithstandvoltage",
            "ratedimpulsewithstandvoltage",
            "impulsewithstandvoltage1250msfullwavekv",
            "impulsewithstandvoltage1250msfullwave",
        ],
        "powerfrequencywithstandvol": [
            "powerfrequencywithstandvoltage",
            "powerfrequencywithstandvoltage1minprimaryside",
            "powerfrequencywithstandvoltage1minute",
            "powerfrequencywithstandvoltage1min",
            "powerfrequencywithstandvoltageprimary",
        ],
        "insulationlvkv": ["insulationlv", "insulation lv"],
    }
    # Merge in persisted aliases from file
    file_aliases = load_file_aliases()
    for k, vals in file_aliases.items():
        alias_map.setdefault(k, [])
        alias_map[k].extend([v for v in vals if v not in alias_map[k]])

    def _tokenize(text: str) -> set[str]:
        cleaned = re.sub(r"[^a-z0-9]+", " ", str(text).lower())
        return {tok for tok in cleaned.split() if tok}

    def _variants(norm: str) -> set[str]:
        variants = {norm}
        if norm.endswith("ies") and len(norm) > 4:
            variants.add(norm[:-3] + "y")
        if norm.endswith("s") and len(norm) > 3:
            variants.add(norm[:-1])
        elif len(norm) > 3:
            variants.add(norm + "s")
        if "manufacturer" in norm:
            variants.add(norm.replace("manufacturer", "manufacture"))
        if "manufacture" in norm:
            variants.add(norm.replace("manufacture", "manufacturer"))
        return {v for v in variants if v}

    norm_target = {normalize_for_compare(t): t for t in target_fields}
    alias_norm = {normalize_for_compare(k): [normalize_for_compare(v) for v in vals] for k, vals in alias_map.items()}

    # Enrich aliases using sample GPKG columns
    dynamic_alias: dict[str, set[str]] = {nt: set() for nt in norm_target}
    ref_cols = get_reference_columns()
    for col in ref_cols:
        norm_col = normalize_for_compare(col)
        tokens_col = _tokenize(col)
        best_nt = None
        best_score = 0.0
        for nt in norm_target:
            score = difflib.SequenceMatcher(None, norm_col, nt).ratio()
            if norm_col and nt and (norm_col in nt or nt in norm_col):
                score = max(score, 0.9)
            if tokens_col and _tokenize(nt):
                overlap = len(tokens_col & _tokenize(nt)) / max(len(tokens_col | _tokenize(nt)), 1)
                score = max(score, overlap)
            if score > best_score:
                best_score = score
                best_nt = nt
        if best_nt and best_score >= 0.8:
            dynamic_alias.setdefault(best_nt, set()).add(norm_col)

    target_meta = {
        tname: {
            "norm": nt,
            "variants": _variants(nt),
            "tokens": _tokenize(tname),
            "aliases": set(alias_norm.get(nt, [])) | dynamic_alias.get(nt, set()),
        }
        for nt, tname in norm_target.items()
    }

    result: dict[str, str] = {}
    result_scores: dict[str, float] = {}
    for src in source_cols:
        if src in exclude:
            continue
        norm_src = normalize_for_compare(src)
        src_variants = _variants(norm_src)
        src_tokens = _tokenize(src)
        best = None
        best_score = threshold
        for tname, meta in target_meta.items():
            score = 0.0
            if meta["aliases"] and any(v in meta["aliases"] for v in src_variants):
                score = max(score, 0.97)
            for sv in src_variants:
                for tv in meta["variants"]:
                    if not sv and not tv:
                        continue
                    ratio = difflib.SequenceMatcher(None, sv, tv).ratio()
                    if sv and tv and (sv in tv or tv in sv):
                        ratio = max(ratio, 0.92)
                    score = max(score, ratio)
            if src_tokens and meta["tokens"]:
                overlap = len(src_tokens & meta["tokens"]) / max(len(src_tokens | meta["tokens"]), 1)
                if overlap:
                    token_score = overlap + (0.05 if overlap == 1 else 0)
                    score = max(score, token_score)
            score = min(score, 1.0)
            if score > best_score or (best is None and score >= threshold) or (
                abs(score - best_score) < 1e-6 and best and len(tname) > len(best)
            ):
                best = tname
                best_score = score
        if best:
            prev = result_scores.get(best, -1)
            if (
                best not in result
                or best_score > prev + 1e-6
                or (abs(best_score - prev) < 1e-6 and len(src) < len(result.get(best, src + "x")))
            ):
                result[best] = src
                result_scores[best] = best_score
    return result


def fuzzy_map_columns_with_scores(
    source_cols: list[str], target_fields: list[str], threshold: float = 0.6, exclude: set[str] | None = None
) -> tuple[dict[str, str], dict[str, float]]:
    """Variant of fuzzy_map_columns that also returns the best score per target."""
    mapping = {}
    scores = {}
    exclude = exclude or set()
    alias_map = fuzzy_map_columns(source_cols, target_fields, threshold, exclude=exclude)  # reuse alias enrichment side effects
    # The above call already computed mapping; to get scores, recompute with slight refactor
    # (keeping logic in sync with fuzzy_map_columns).

    # Rebuild enriched metadata (copied logic)
    base_alias = {
        "countryofmanufacturer": ["manufacturingcountry", "countryofmanufacturing", "countryoforigin", "countryofmanufacture"],
        "countryofmanufacture": ["countryofmanufacturer", "countrymanufacturer"],
        "manufacturer": ["manufactoringcompany", "manufacturingcompany"],
        "manufactureryear": ["manufacturingyear", "yearofmanufacturer", "manufacturing_year"],
        "temperature range": ["temperaturerange", "temperature_range"],
        "typemodel": ["type_model", "type/model", "type model", "type-model"],
        "standards": ["standard", "std"],
        "standard": ["standards", "std"],
        "light_impulse_withsand_kv": [
            "impulsewithstandvoltage",
            "impulsewithstand",
            "impulsewithstandvoltage1250msfullwavekv",
            "impulsewithstandvoltage1250msfullwave",
            "impulsewithstandvoltagepeak",
        ],
        "ratedimpulsewithstandvol": [
            "impulsewithstandvoltage",
            "ratedimpulsewithstandvoltage",
            "impulsewithstandvoltage1250msfullwavekv",
            "impulsewithstandvoltage1250msfullwave",
        ],
        "powerfrequencywithstandvol": [
            "powerfrequencywithstandvoltage",
            "powerfrequencywithstandvoltage1minprimaryside",
            "powerfrequencywithstandvoltage1minute",
            "powerfrequencywithstandvoltage1min",
            "powerfrequencywithstandvoltageprimary",
        ],
    }
    file_aliases = load_file_aliases()
    for k, vals in file_aliases.items():
        base_alias.setdefault(k, [])
        base_alias[k].extend([v for v in vals if v not in base_alias[k]])

    def _tokenize(text: str) -> set[str]:
        cleaned = re.sub(r"[^a-z0-9]+", " ", str(text).lower())
        return {tok for tok in cleaned.split() if tok}

    def _variants(norm: str) -> set[str]:
        variants = {norm}
        if norm.endswith("ies") and len(norm) > 4:
            variants.add(norm[:-3] + "y")
        if norm.endswith("s") and len(norm) > 3:
            variants.add(norm[:-1])
        elif len(norm) > 3:
            variants.add(norm + "s")
        if "manufacturer" in norm:
            variants.add(norm.replace("manufacturer", "manufacture"))
        if "manufacture" in norm:
            variants.add(norm.replace("manufacture", "manufacturer"))
        return {v for v in variants if v}

    norm_target = {normalize_for_compare(t): t for t in target_fields}
    alias_norm = {normalize_for_compare(k): [normalize_for_compare(v) for v in vals] for k, vals in base_alias.items()}

    dynamic_alias: dict[str, set[str]] = {nt: set() for nt in norm_target}
    ref_cols = get_reference_columns()
    for col in ref_cols:
        norm_col = normalize_for_compare(col)
        tokens_col = _tokenize(col)
        best_nt = None
        best_score = 0.0
        for nt in norm_target:
            score = difflib.SequenceMatcher(None, norm_col, nt).ratio()
            if norm_col and nt and (norm_col in nt or nt in norm_col):
                score = max(score, 0.9)
            if tokens_col and _tokenize(nt):
                overlap = len(tokens_col & _tokenize(nt)) / max(len(tokens_col | _tokenize(nt)), 1)
                score = max(score, overlap)
            if score > best_score:
                best_score = score
                best_nt = nt
        if best_nt and best_score >= 0.8:
            dynamic_alias.setdefault(best_nt, set()).add(norm_col)

    target_meta = {
        tname: {
            "norm": nt,
            "variants": _variants(nt),
            "tokens": _tokenize(tname),
            "aliases": set(alias_norm.get(nt, [])) | dynamic_alias.get(nt, set()),
        }
        for nt, tname in norm_target.items()
    }

    result: dict[str, str] = {}
    result_scores: dict[str, float] = {}
    for src in source_cols:
        if src in exclude:
            continue
        norm_src = normalize_for_compare(src)
        src_variants = _variants(norm_src)
        src_tokens = _tokenize(src)
        best = None
        best_score = threshold
        for tname, meta in target_meta.items():
            score = 0.0
            if meta["aliases"] and any(v in meta["aliases"] for v in src_variants):
                score = max(score, 0.97)
            for sv in src_variants:
                for tv in meta["variants"]:
                    if not sv and not tv:
                        continue
                    ratio = difflib.SequenceMatcher(None, sv, tv).ratio()
                    if sv and tv and (sv in tv or tv in sv):
                        ratio = max(ratio, 0.92)
                    score = max(score, ratio)
            if src_tokens and meta["tokens"]:
                overlap = len(src_tokens & meta["tokens"]) / max(len(src_tokens | meta["tokens"]), 1)
                if overlap:
                    token_score = overlap + (0.05 if overlap == 1 else 0)
                    score = max(score, token_score)
            score = min(score, 1.0)
            if score > best_score or (best is None and score >= threshold) or (
                abs(score - best_score) < 1e-6 and best and len(tname) > len(best)
            ):
                best = tname
                best_score = score
        if best:
            prev = result_scores.get(best, -1)
            if (
                best not in result
                or best_score > prev + 1e-6
                or (abs(best_score - prev) < 1e-6 and len(src) < len(result.get(best, src + "x")))
            ):
                result[best] = src
                result_scores[best] = best_score

    mapping = result
    scores = result_scores
    return mapping, scores


def assign_ct_labels(
    gdf: gpd.GeoDataFrame,
    sub_col: str,
    sub_value: str,
    type_col: str,
    ct_keywords: list[str],
    transformer_keywords: list[str],
    output_field: str = "CT_LABEL",
) -> gpd.GeoDataFrame:
    """Assign CT labels (CT1, CT2, ...) based on proximity to transformers within a substation."""
    working = gdf.copy()
    # Filter to target substation
    norm_sub = normalize_value_for_compare(sub_value)
    norm_col = working[sub_col].map(normalize_value_for_compare)
    mask_sub = (norm_col == norm_sub).fillna(False)
    sub_gdf = working.loc[mask_sub].copy()

    if sub_gdf.empty or type_col not in sub_gdf.columns:
        return working

    norm_types = sub_gdf[type_col].fillna("").map(normalize_value_for_compare)
    transformer_mask = norm_types.apply(lambda v: any(kw in v for kw in transformer_keywords))
    ct_mask = norm_types.apply(lambda v: any(kw in v for kw in ct_keywords))

    transformers = sub_gdf.loc[transformer_mask].copy()
    cts = sub_gdf.loc[ct_mask].copy()

    if transformers.empty or cts.empty:
        return working

    # Work in metric for distance
    transformers_m = to_metric(transformers)
    cts_m = to_metric(cts)

    transformer_geom = transformers_m.geometry.reset_index(drop=True)
    ct_geom = cts_m.geometry.reset_index(drop=True)
    if transformer_geom.is_empty.all() or ct_geom.is_empty.all():
        return working

    distances = []
    for ct_idx, geom in enumerate(ct_geom):
        if geom is None or geom.is_empty:
            distances.append((ct_idx, None, None))
            continue
        dists = transformer_geom.distance(geom)
        nearest_idx = dists.idxmin()
        distances.append((ct_idx, nearest_idx, dists.iloc[nearest_idx]))

    ranked = sorted([t for t in distances if t[2] is not None], key=lambda x: (x[2], x[0]))
    labels = {}
    for rank, (ct_idx, _, _) in enumerate(ranked, start=1):
        labels[ct_idx] = f"CT{rank}"

    cts[output_field] = [labels.get(i, None) for i in range(len(cts))]

    working.loc[cts.index, output_field] = cts[output_field].values
    return working


def order_indices_by_location(geom: gpd.GeoSeries) -> list[int]:
    """Return geometry indices ordered by location using a dominant-axis sort with band grouping."""
    if geom is None:
        return []
    coords: list[tuple[int, float, float]] = []
    missing: list[int] = []
    for idx, g in geom.items():
        if g is None or getattr(g, "is_empty", True):
            missing.append(idx)
            continue
        try:
            pt = g if getattr(g, "geom_type", "") == "Point" else g.centroid
        except Exception:
            missing.append(idx)
            continue
        if pt is None or getattr(pt, "is_empty", True):
            missing.append(idx)
            continue
        try:
            x = float(pt.x)
            y = float(pt.y)
        except Exception:
            missing.append(idx)
            continue
        coords.append((idx, x, y))

    if len(coords) <= 1:
        return [idx for idx, _, _ in coords] + missing

    xs = [x for _, x, _ in coords]
    ys = [y for _, _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]

    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)

    if var_x < 1e-12 and var_y < 1e-12:
        ordered = sorted(coords, key=lambda t: (t[2], t[1]))
        return [idx for idx, _, _ in ordered] + missing

    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)

    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0

    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm

    # Orient axis to keep ordering stable (north/east positive).
    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy

    along_perp: list[tuple[int, float, float]] = []
    for idx, x, y in coords:
        dx = x - mean_x
        dy = y - mean_y
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        along_perp.append((idx, along, perp))

    perp_sorted = sorted(along_perp, key=lambda t: t[2])
    perps = [p for _, _, p in perp_sorted]
    if len(perps) < 2:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    diffs = [perps[i + 1] - perps[i] for i in range(len(perps) - 1)]
    diffs_sorted = sorted(diffs)
    median_diff = diffs_sorted[len(diffs_sorted) // 2]
    abs_dev = [abs(d - median_diff) for d in diffs_sorted]
    mad = sorted(abs_dev)[len(abs_dev) // 2] if abs_dev else 0.0
    # Only split when gaps are clearly larger than the typical spacing.
    gap_threshold = max(median_diff * 3, median_diff + 3 * mad)

    if gap_threshold <= 0:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    groups: list[list[tuple[int, float, float]]] = []
    current: list[tuple[int, float, float]] = []
    last_perp: float | None = None
    for item in perp_sorted:
        if last_perp is None:
            current = [item]
        elif item[2] - last_perp > gap_threshold:
            groups.append(current)
            current = [item]
        else:
            current.append(item)
        last_perp = item[2]
    if current:
        groups.append(current)

    if len(groups) <= 1:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    def _group_median(group: list[tuple[int, float, float]]) -> float:
        return statistics.median([p[2] for p in group])

    ordered_indices: list[int] = []
    for group in sorted(groups, key=_group_median):
        group_sorted = sorted(group, key=lambda t: t[1])
        ordered_indices.extend([idx for idx, _, _ in group_sorted])

    return ordered_indices + missing


def group_indices_by_perp_gap(geom: gpd.GeoSeries, group_count: int) -> dict[int, int]:
    """Group geometry indices into contiguous bands based on perpendicular gaps."""
    if geom is None or group_count <= 0:
        return {}

    coords: list[tuple[int, float, float]] = []
    missing: list[int] = []
    for idx, g in geom.items():
        if g is None or getattr(g, "is_empty", True):
            missing.append(idx)
            continue
        try:
            pt = g if getattr(g, "geom_type", "") == "Point" else g.centroid
        except Exception:
            missing.append(idx)
            continue
        if pt is None or getattr(pt, "is_empty", True):
            missing.append(idx)
            continue
        try:
            x = float(pt.x)
            y = float(pt.y)
        except Exception:
            missing.append(idx)
            continue
        coords.append((idx, x, y))

    if len(coords) <= 1:
        mapping = {idx: 0 for idx, _, _ in coords}
        for idx in missing:
            mapping[idx] = 0
        return mapping

    xs = [x for _, x, _ in coords]
    ys = [y for _, _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]

    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)

    if var_x < 1e-12 and var_y < 1e-12:
        ordered = sorted(coords, key=lambda t: (t[2], t[1]))
        mapping = {idx: 0 for idx, _, _ in ordered}
        for idx in missing:
            mapping[idx] = 0
        return mapping

    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)

    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0

    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm

    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy

    items: list[tuple[int, float, float]] = []
    for idx, x, y in coords:
        dx = x - mean_x
        dy = y - mean_y
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        items.append((idx, along, perp))

    items.sort(key=lambda t: t[2])
    group_count = max(1, min(group_count, len(items)))

    groups: list[list[tuple[int, float, float]]] = [[item] for item in items]
    # Merge the closest neighboring bands until we have the requested count.
    while len(groups) > group_count:
        gaps = [
            groups[i + 1][0][2] - groups[i][-1][2]
            for i in range(len(groups) - 1)
        ]
        merge_idx = gaps.index(min(gaps))
        groups[merge_idx].extend(groups[merge_idx + 1])
        del groups[merge_idx + 1]

    mapping: dict[int, int] = {}
    for group_id, group in enumerate(groups):
        group_sorted = sorted(group, key=lambda t: t[1])
        for idx, _, _ in group_sorted:
            mapping[idx] = group_id
    for idx in missing:
        mapping[idx] = min(group_count - 1, len(groups) - 1)
    return mapping


def resolve_ups_anchor_point(ups_path: Path, ups_layer: str | None, target_crs) -> Any:
    """Return a Point anchor from an UPS GeoPackage layer."""
    if ups_path is None or ups_layer is None:
        return None
    try:
        ups_gdf = gpd.read_file(ups_path, layer=ups_layer)
    except Exception:
        return None
    if ups_gdf.empty or not hasattr(ups_gdf, "geometry"):
        return None
    if target_crs is not None and ups_gdf.crs is not None and ups_gdf.crs != target_crs:
        try:
            ups_gdf = ups_gdf.to_crs(target_crs)
        except Exception:
            pass
    for geom in ups_gdf.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        try:
            return geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
        except Exception:
            continue
    return None


def load_ups_anchor_and_crs(ups_path: Path, ups_layer: str | None) -> tuple[Any, Any]:
    """Return a Point anchor and CRS from an UPS GeoPackage layer."""
    if ups_path is None or ups_layer is None:
        return None, None
    try:
        ups_gdf = gpd.read_file(ups_path, layer=ups_layer)
    except Exception:
        return None, None
    if ups_gdf.empty or not hasattr(ups_gdf, "geometry"):
        return None, ups_gdf.crs
    for geom in ups_gdf.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        try:
            anchor = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
            return anchor, ups_gdf.crs
        except Exception:
            continue
    return None, ups_gdf.crs


def build_protection_layout_points(anchor: Any, count: int, spacing: float) -> list[Any]:
    """Build protection points in a 2xN grid below the anchor point."""
    if anchor is None or count <= 0:
        return []
    try:
        x = float(anchor.x)
        y = float(anchor.y)
    except Exception:
        return []
    if spacing <= 0:
        spacing = PROTECTION_LAYOUT_SPACING
    try:
        from shapely.geometry import Point
    except Exception:
        return []

    points: list[Any] = []
    if count == 1:
        points.append(Point(x, y - spacing))
        return points
    if count == 2:
        points.append(Point(x - spacing * 0.5, y - spacing))
        points.append(Point(x + spacing * 0.5, y - spacing))
        return points

    for i in range(count):
        row = i // 2
        col = i % 2
        x_off = (-0.5 + col) * spacing
        y_off = -(row + 1) * spacing
        points.append(Point(x + x_off, y + y_off))
    return points


def build_device_gdf_from_instances(
    instances: list[dict[str, Any]],
    points: list[Any],
    crs,
) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame with instance fields aligned to generated points."""
    count = len(points)
    if count <= 0:
        return gpd.GeoDataFrame(geometry=[])
    fields_ordered: list[str] = []
    fields_seen: set[str] = set()
    for inst in instances:
        for f in inst.get("order", []) or []:
            if f not in fields_seen:
                fields_seen.add(f)
                fields_ordered.append(f)
        for f in (inst.get("fields", {}) or {}).keys():
            if f not in fields_seen:
                fields_seen.add(f)
                fields_ordered.append(f)
    data: dict[str, list[Any]] = {f: [pd.NA] * count for f in fields_ordered}
    for idx, inst in enumerate(instances):
        if idx >= count:
            break
        fields = inst.get("fields", {}) or {}
        for f, val in fields.items():
            if f not in data:
                data[f] = [pd.NA] * count
            fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
            data[f][idx] = fill_val
    out_gdf = gpd.GeoDataFrame(data, geometry=points, crs=crs)
    return sanitize_gdf_for_gpkg(out_gdf)


def repeat_instances(instances: list[dict[str, Any]], repeat_count: int) -> list[dict[str, Any]]:
    """Repeat each instance in order to match a target count per instance."""
    if repeat_count <= 0:
        return []
    expanded: list[dict[str, Any]] = []
    for inst in instances:
        for _ in range(repeat_count):
            expanded.append(inst)
    return expanded


def _get_polygon_coords(geom: Any) -> list[tuple[float, float]]:
    if geom is None or getattr(geom, "is_empty", True):
        return []
    try:
        geom_type = getattr(geom, "geom_type", "")
        if geom_type == "Polygon":
            return [(float(x), float(y)) for x, y in geom.exterior.coords]
        if geom_type == "MultiPolygon":
            poly = max(list(geom.geoms), key=lambda g: g.area, default=None)
            if poly is None:
                return []
            return [(float(x), float(y)) for x, y in poly.exterior.coords]
    except Exception:
        return []
    return []


def _dominant_axis_from_coords(coords: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    xs = [x for x, _ in coords]
    ys = [y for _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]
    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)
    if var_x + var_y < 1e-12:
        return 1.0, 0.0, mean_x, mean_y
    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)
    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0
    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm
    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy
    return ux, uy, mean_x, mean_y


def build_parallel_lines_for_polygon(geom: Any, count: int) -> list[Any]:
    """Create parallel lines that cross a polygon along its dominant axis."""
    if count <= 0:
        return []
    coords = _get_polygon_coords(geom)
    if not coords:
        return []
    try:
        from shapely.geometry import LineString
    except Exception:
        return []
    ux, uy, cx, cy = _dominant_axis_from_coords(coords)
    px, py = -uy, ux
    alongs: list[float] = []
    perps: list[float] = []
    for x, y in coords:
        dx = x - cx
        dy = y - cy
        alongs.append(dx * ux + dy * uy)
        perps.append(-dx * uy + dy * ux)
    min_along = min(alongs)
    max_along = max(alongs)
    min_perp = min(perps)
    max_perp = max(perps)
    length = max_along - min_along
    if length <= 0:
        return []
    margin = length * 0.05
    span = max_perp - min_perp
    if span <= 0:
        offsets = [min_perp] * count
    else:
        offsets = [min_perp + (i + 1) * span / (count + 1) for i in range(count)]
    lines: list[Any] = []
    for off in offsets:
        a0 = min_along - margin
        a1 = max_along + margin
        x0 = cx + a0 * ux + off * px
        y0 = cy + a0 * uy + off * py
        x1 = cx + a1 * ux + off * px
        y1 = cy + a1 * uy + off * py
        lines.append(LineString([(x0, y0), (x1, y1)]))
    return lines


@st.cache_data(show_spinner=False)
def load_template_layer(path: Path) -> tuple[gpd.GeoDataFrame, str] | None:
    """Load the first layer from a template GeoPackage for geometry placement."""
    if path is None or not path.exists():
        return None
    layers = list_gpkg_layers(path)
    if not layers:
        return None
    layer = layers[0]
    try:
        gdf = gpd.read_file(path, layer=layer)
    except Exception:
        return None
    if gdf.empty or not hasattr(gdf, "geometry"):
        return None
    return gdf[[gdf.geometry.name]].copy(), layer


def expand_geometries(geoms: list[Any], target_count: int) -> list[Any]:
    """Expand or trim geometry list to match the target count (repeat if needed)."""
    if target_count <= 0 or not geoms:
        return []
    if len(geoms) >= target_count:
        return geoms[:target_count]
    expanded: list[Any] = []
    idx = 0
    while len(expanded) < target_count:
        expanded.append(geoms[idx % len(geoms)])
        idx += 1
    return expanded


@st.cache_data(show_spinner=False)
def load_line_bay_layer(path: Path, layer: str | None, field: str | None) -> gpd.GeoDataFrame | None:
    """Load line bay polygons with the selected name field."""
    if path is None or layer is None or field is None:
        return None
    try:
        gdf = gpd.read_file(path, layer=layer)
    except Exception:
        return None
    if gdf.empty or not hasattr(gdf, "geometry"):
        return None
    if field not in gdf.columns:
        return None
    geom_col = gdf.geometry.name
    try:
        gdf = gdf[gdf[geom_col].notna() & ~gdf[geom_col].is_empty]
    except Exception:
        pass
    if gdf.empty:
        return None
    return gdf[[field, geom_col]].copy().reset_index(drop=True)


def collect_point_geometries_from_uploads(
    files: list[Any] | None,
    target_crs,
) -> gpd.GeoDataFrame | None:
    """Collect point geometries from uploaded GeoPackages."""
    if not files:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                tmp.write(file_obj.getbuffer())
                gpkg_path = Path(tmp.name)
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                point_mask = geom_types.isin(["Point", "MultiPoint"])
                if not bool(point_mask.any()):
                    continue
                gdf_pts = gdf.loc[point_mask].copy()
                try:
                    if (gdf_pts.geometry.geom_type == "MultiPoint").any():
                        gdf_pts = gdf_pts.explode(index_parts=False)
                except Exception:
                    pass
                if target_crs is not None and gdf_pts.crs is not None and gdf_pts.crs != target_crs:
                    try:
                        gdf_pts = gdf_pts.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_pts.geometry, crs=target_crs or gdf_pts.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)


def collect_device_points_from_uploads(
    files: list[Any] | None,
    target_crs,
    device_options: list[str],
    equip_map: dict[str, str],
    target_device_norms: set[str],
) -> gpd.GeoDataFrame | None:
    """Collect point geometries from uploads for specific devices (e.g., Lightning Arrestor)."""
    if not files or not target_device_norms:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            dev_name = resolve_equipment_name(file_obj.name, device_options, equip_map)
        except Exception:
            dev_name = None
        if normalize_for_compare(dev_name) not in target_device_norms:
            continue
        try:
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                tmp.write(file_obj.getbuffer())
                gpkg_path = Path(tmp.name)
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                point_mask = geom_types.isin(["Point", "MultiPoint"])
                if not bool(point_mask.any()):
                    continue
                gdf_pts = gdf.loc[point_mask].copy()
                try:
                    if (gdf_pts.geometry.geom_type == "MultiPoint").any():
                        gdf_pts = gdf_pts.explode(index_parts=False)
                except Exception:
                    pass
                if target_crs is not None and gdf_pts.crs is not None and gdf_pts.crs != target_crs:
                    try:
                        gdf_pts = gdf_pts.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_pts.geometry, crs=target_crs or gdf_pts.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)


def collect_device_polygons_from_uploads(
    files: list[Any] | None,
    target_crs,
    device_options: list[str],
    equip_map: dict[str, str],
    target_device_norms: set[str],
) -> gpd.GeoDataFrame | None:
    """Collect polygon geometries from uploads for specific devices (e.g., Cabins)."""
    if not files or not target_device_norms:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            dev_name = resolve_equipment_name(file_obj.name, device_options, equip_map)
        except Exception:
            dev_name = None
        if normalize_for_compare(dev_name) not in target_device_norms:
            continue
        try:
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                tmp.write(file_obj.getbuffer())
                gpkg_path = Path(tmp.name)
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                poly_mask = geom_types.isin(["Polygon", "MultiPolygon"])
                if not bool(poly_mask.any()):
                    continue
                gdf_poly = gdf.loc[poly_mask].copy()
                if target_crs is not None and gdf_poly.crs is not None and gdf_poly.crs != target_crs:
                    try:
                        gdf_poly = gdf_poly.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_poly.geometry, crs=target_crs or gdf_poly.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)


def map_points_to_bays(
    points_gdf: gpd.GeoDataFrame | None,
    bay_gdf: gpd.GeoDataFrame,
) -> dict[int, list[Any]]:
    """Map points to line bay polygon indices (intersects or near-touch, ordered shallowest points first)."""
    if points_gdf is None or points_gdf.empty:
        return {}
    if points_gdf.crs is not None and bay_gdf.crs is not None and points_gdf.crs != bay_gdf.crs:
        try:
            points_gdf = points_gdf.to_crs(bay_gdf.crs)
        except Exception:
            pass
    joined = None
    try:
        joined = gpd.sjoin(points_gdf, bay_gdf, how="left", predicate="intersects")
    except TypeError:
        try:
            joined = gpd.sjoin(points_gdf, bay_gdf, how="left", op="intersects")
        except Exception:
            joined = None
    except Exception:
        joined = None
    if joined is None or "index_right" not in joined.columns:
        out: dict[int, list[Any]] = {}
        try:
            bay_items = list(bay_gdf.geometry.items())
        except Exception:
            bay_items = []
        for pt in points_gdf.geometry:
            if pt is None or getattr(pt, "is_empty", True):
                continue
            for idx, poly in bay_items:
                if poly is None or getattr(poly, "is_empty", True):
                    continue
                try:
                    if poly.intersects(pt):
                        try:
                            key = int(idx)
                        except Exception:
                            key = idx
                        out.setdefault(key, []).append(pt)
                        break
                except Exception:
                    continue
        return out
    out: dict[int, list[Any]] = {}
    used_point_idx: set[int] = set()
    if joined is not None:
        for idx, row in joined.iterrows():
            bay_idx = row.get("index_right")
            if pd.isna(bay_idx):
                continue
            try:
                bay_key = int(bay_idx)
            except Exception:
                continue
            out.setdefault(bay_key, []).append((idx, row.geometry))
            used_point_idx.add(idx)

    bay_geoms: list[tuple[int, Any, float]] = []
    try:
        for idx, geom in bay_gdf.geometry.items():
            if geom is None or getattr(geom, "is_empty", True):
                continue
            try:
                width = geom.bounds[2] - geom.bounds[0]
                height = geom.bounds[3] - geom.bounds[1]
                min_dim = min(width, height)
            except Exception:
                min_dim = 0.0
            tol = max(0.1, min_dim * 0.15)
            bay_geoms.append((idx, geom, tol))
    except Exception:
        bay_geoms = []

    if bay_geoms:
        for idx_pt, pt in enumerate(points_gdf.geometry):
            if idx_pt in used_point_idx:
                continue
            if pt is None or getattr(pt, "is_empty", True):
                continue
            best_idx = None
            best_dist = None
            for bay_idx, bay_geom, tol in bay_geoms:
                try:
                    dist = bay_geom.distance(pt)
                except Exception:
                    continue
                if dist <= tol and (best_dist is None or dist < best_dist):
                    best_dist = dist
                    best_idx = bay_idx
            if best_idx is not None:
                try:
                    bay_key = int(best_idx)
                except Exception:
                    bay_key = best_idx
                out.setdefault(bay_key, []).append((idx_pt, pt))

    ordered_out: dict[int, list[Any]] = {}
    for bay_idx, items in out.items():
        sorted_items = sorted(items, key=lambda t: t[0])
        ordered_out[bay_idx] = [geom for _, geom in sorted_items]
    return ordered_out


def group_points_by_perp_gap(
    items: list[tuple[Any, float, float]],
    group_count: int,
) -> list[list[tuple[Any, float, float]]]:
    """Group items by closest gaps along perpendicular coordinate."""
    if not items or group_count <= 0:
        return []
    group_count = min(group_count, len(items))
    items_sorted = sorted(items, key=lambda t: (t[2], t[1]))
    groups: list[list[tuple[Any, float, float]]] = [[item] for item in items_sorted]
    while len(groups) > group_count:
        gaps = [
            groups[i + 1][0][2] - groups[i][-1][2]
            for i in range(len(groups) - 1)
        ]
        merge_idx = gaps.index(min(gaps))
        groups[merge_idx].extend(groups[merge_idx + 1])
        del groups[merge_idx + 1]
    return groups


def build_lines_from_points_in_polygon(
    polygon: Any,
    points: list[Any],
    count: int,
) -> list[Any]:
    """Build line strings for a polygon using internal points, fallback to parallel lines."""
    if count <= 0:
        return []
    coords = _get_polygon_coords(polygon)
    if not coords:
        return []
    if not points:
        return build_parallel_lines_for_polygon(polygon, count)
    try:
        from shapely.geometry import LineString, Point
    except Exception:
        return []
    ux, uy, cx, cy = _dominant_axis_from_coords(coords)
    alongs: list[float] = []
    for x, y in coords:
        dx = x - cx
        dy = y - cy
        alongs.append(dx * ux + dy * uy)
    if not alongs:
        return []
    min_along = min(alongs)
    max_along = max(alongs)
    items: list[tuple[Any, float, float]] = []
    for pt in points:
        if pt is None or getattr(pt, "is_empty", True):
            continue
        try:
            p = pt if getattr(pt, "geom_type", "") == "Point" else pt.centroid
            x = float(p.x)
            y = float(p.y)
        except Exception:
            continue
        dx = x - cx
        dy = y - cy
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        items.append((p, along, perp))
    if len(items) < count:
        return build_parallel_lines_for_polygon(polygon, count)
    groups = group_points_by_perp_gap(items, count)
    if len(groups) < count:
        return build_parallel_lines_for_polygon(polygon, count)
    margin = (max_along - min_along) * 0.05
    px, py = -uy, ux
    lines: list[Any] = []
    for group in groups:
        group_sorted = sorted(group, key=lambda t: t[1])
        if not group_sorted:
            continue
        if len(group_sorted) > 2:
            group_sorted = [group_sorted[0], group_sorted[-1]]
        avg_perp = sum(item[2] for item in group_sorted) / len(group_sorted)
        group_mean_along = sum(item[1] for item in group_sorted) / len(group_sorted)
        dist_to_min = abs(group_mean_along - min_along)
        dist_to_max = abs(max_along - group_mean_along)
        extend_min_side = dist_to_min <= dist_to_max
        if not extend_min_side:
            group_sorted = list(reversed(group_sorted))
        extend_along = (min_along - margin * 2) if extend_min_side else (max_along + margin * 2)
        start_pt = Point(cx + extend_along * ux + avg_perp * px, cy + extend_along * uy + avg_perp * py)
        path = [start_pt] + [item[0] for item in group_sorted]
        lines.append(LineString([(p.x, p.y) for p in path]))
    if len(lines) != count:
        return build_parallel_lines_for_polygon(polygon, count)
    return lines


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


@st.cache_data(show_spinner=False)
def load_domain_code_lookup() -> dict[str, Any]:
    """Build a global mapping of domain text -> domain code from supervisor workbooks."""
    lookup: dict[str, Any] = {}
    if not SUPERVISOR_WORKBOOK_DIR.exists():
        return lookup
    workbooks = [p for p in SUPERVISOR_WORKBOOK_DIR.glob("**/*") if p.is_file() and p.suffix.lower() in REFERENCE_EXTENSIONS]
    for wb_path in sorted(workbooks):
        try:
            xl = pd.ExcelFile(wb_path)
        except Exception:
            continue
        for sheet in xl.sheet_names:
            try:
                raw = pd.read_excel(wb_path, sheet_name=sheet, dtype=str, header=None)
            except Exception:
                continue
            if raw.empty or raw.shape[1] < 5:
                continue
            for _, row in raw.iterrows():
                dom_val = row.iloc[3]
                code_val = row.iloc[4]
                if pd.isna(dom_val) or pd.isna(code_val):
                    continue
                dom_norm = normalize_value_for_compare(dom_val)
                if dom_norm and dom_norm not in lookup:
                    lookup[dom_norm] = code_val
    return lookup


def build_spatial_match_targets(
    line_gdf: gpd.GeoDataFrame,
    bay_path: Path,
    bay_layer: str | None,
    bay_field: str | None,
) -> pd.Series:
    """Return normalized match targets for each line feature based on Line Bay polygons."""
    if line_gdf is None or line_gdf.empty or bay_path is None or bay_layer is None or bay_field is None:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)
    try:
        bay_gdf = gpd.read_file(bay_path, layer=bay_layer)
    except Exception:
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)
    if bay_field not in bay_gdf.columns or not hasattr(bay_gdf, "geometry"):
        return pd.Series([pd.NA] * len(line_gdf), index=line_gdf.index)

    geom_name = bay_gdf.geometry.name
    bay = bay_gdf[[bay_field, geom_name]].copy()
    try:
        bay = bay[bay[geom_name].notna() & ~bay[geom_name].is_empty]
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

    field_name = bay_field
    if field_name not in joined.columns:
        alt = f"{bay_field}_bay"
        field_name = alt if alt in joined.columns else bay_field

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
    return series.map(normalize_value_for_compare)


def load_schema_fields(
    schema_path: Path,
    sheet_name: str,
    equipment_name: str | None,
    header_row: int | None = None,
    device_col: int = 0,
    field_col: int | None = None,
    type_col: int | None = None,
) -> tuple[list[str], dict[str, str]]:
    """Load field names and types for a specific equipment/device from a schema sheet.
    If equipment_name is None, returns all fields in the sheet."""
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

    # Ensure columns exist
    while schema_df.shape[1] <= max(field_col, type_col):
        schema_df[schema_df.shape[1]] = None

    schema_df.columns = [f"col_{i}" for i in range(schema_df.shape[1])]
    field_series = schema_df.iloc[:, field_col]
    type_series = schema_df.iloc[:, type_col]

    schema_df = pd.DataFrame({"field": field_series, "type": type_series})
    schema_df["field"] = schema_df["field"].fillna("").map(_clean_column_name)
    schema_df["type"] = schema_df["type"].fillna("").map(str)
    schema_df = schema_df[schema_df["field"] != ""]
    schema_df = schema_df[
        schema_df["field"].map(lambda x: normalize_for_compare(x) not in ("field", "fieldname"))
    ]
    fields = schema_df["field"].tolist()
    type_map = dict(zip(schema_df["field"], schema_df["type"]))
    return fields, type_map


def load_reference_sheet(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    """Load and clean a sheet from the reference workbook using the same logic as the main loader."""
    cache_key = (_cache_key_from_path(workbook_path), sheet_name)
    cached = _REFERENCE_SHEET_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    excel_file = get_excel_file(workbook_path)
    raw_df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str, header=None)
    header_row = _detect_header_row(raw_df)
    header = [_clean_column_name(c) for c in raw_df.iloc[header_row]]
    header = ensure_unique_columns(header)
    df = raw_df.iloc[header_row + 1 :].copy()
    df.columns = header
    df.reset_index(drop=True, inplace=True)
    df = _apply_global_forward_fill(df)
    df = clean_empty_rows(df)
    _REFERENCE_SHEET_CACHE[cache_key] = df
    return df.copy()


def list_schema_equipments(schema_path: Path, sheet_name: str, device_col: int = 0) -> list[str]:
    """List unique equipment/device names from a schema sheet."""
    if normalize_for_compare(sheet_name) == normalize_for_compare("Electric device"):
        return ELECTRIC_DEVICE_EQUIPMENT
    schema_raw = pd.read_excel(schema_path, sheet_name=sheet_name, dtype=str, header=None)
    devices = schema_raw.iloc[:, device_col].ffill().dropna().map(_clean_column_name).map(str.strip)
    devices = [d for d in devices if d]
    # skip header-like entries
    devices = [d for d in devices if normalize_for_compare(d) not in ("device", "equipment")]
    return sorted(set(devices))


_NUM_REGEX = re.compile(r"[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?".replace("\\\\", "\\"))


def _extract_first_number(value: Any) -> float | None:
    """Extract the first numeric value from a string; returns None if none found."""
    if pd.isna(value):
        return None
    text = str(value)
    # Normalize minus signs/spaces
    text = text.replace("−", "-")
    m = _NUM_REGEX.search(text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def coerce_series_to_type(series: pd.Series, type_str: str) -> pd.Series:
    """Coerce series to target type based on schema string, with lenient numeric parsing and datetime handling."""
    t = normalize_for_compare(type_str or "")
    if not isinstance(series, pd.Series):
        return series
    if any(tok in t for tok in ("date", "datetime", "timestamp")):
        return pd.to_datetime(series, errors="coerce")
    if any(tok in t for tok in ("int", "integer", "long", "short", "bigint", "smallint")):
        coerced = series.map(_extract_first_number)
        return pd.Series(coerced, dtype="Int64")
    if any(tok in t for tok in ("double", "float", "decimal", "real", "number")):
        coerced = series.map(_extract_first_number)
        return pd.Series(coerced, dtype="float64")
    if "bool" in t:
        try:
            return series.astype("boolean")
        except Exception:
            return series.map(lambda v: str(v).strip().lower() in {"true", "1", "yes"} if pd.notna(v) else pd.NA).astype("boolean")
    # default to string for text-like
    return series.astype("string")


def normalize_for_compare(name: Any) -> str:
    """Prepare string for joining / comparisons by stripping punctuation & spaces."""
    if name is None:
        return ""
    text = str(name).lower()

    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")

    text = " ".join(text.split())
    text = text.translate(COMPARISON_TRANSLATION_TABLE)
    return text.strip()


def normalize_value_for_compare(value: Any) -> str:
    if value is None:
        text = ""
    else:
        try:
            text = "" if pd.isna(value) else str(value)
        except Exception:
            text = str(value)

    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")

    text = text.lower().replace("_", "").replace("-", "")
    return " ".join(text.split()).strip()

# Hard overrides for filename -> device label when heuristics/alias map are insufficient.
FILE_DEVICE_OVERRIDES = {
    normalize_for_compare("BUSBAR1"): "High Voltage Busbar/Medium Voltage Busbar",
    normalize_for_compare("TRANSFORMER"): "Power Transformer/ Stepup Transformer",
    normalize_for_compare("DISCONNECTOR SWITCHES1"): "High Voltage Switch/High Voltage Switch",
    normalize_for_compare("INDOR CB"): "Indoor Circuit Breaker/30kv/15kb",
    normalize_for_compare("INDOR CT"): "Indoor Current Transformer",
    normalize_for_compare("INDOR VT"): "Indoor Voltage Transformer",
    normalize_for_compare("CT INDOR SWITCHGEAR"): "Indoor Current Transformer",
    normalize_for_compare("ct_indor_switchgear"): "Indoor Current Transformer",
    normalize_for_compare("UPS"): "Uninterruptable power supply(UPS)",
    normalize_for_compare("TRANS_SYSTEM PROT2"): "Distance Protection",
    normalize_for_compare("POWER_TRANSFORMER"): "Power Transformer/ Stepup Transformer",
    normalize_for_compare("power_transformer"): "Power Transformer/ Stepup Transformer",
    normalize_for_compare("TELECOM"): "Control and Protection Panels",
    normalize_for_compare("TELECOM_ODF"): "Control and Protection Panels",
}

# Columns to drop from output after filling (utility fields used only for matching).
DROP_OUTPUT_COLUMNS = {
    normalize_for_compare("Composite_ID"),
    normalize_for_compare("Composite ID"),
}

# Devices where, if no matches are found, we distribute sheet instances across rows to keep feature counts.
SEQUENTIAL_FILL_DEVICES = {
    normalize_for_compare("Indoor Circuit Breaker/30kv/15kb"),
    normalize_for_compare("Indoor Current Transformer"),
    normalize_for_compare("Indoor Voltage Transformer"),
}

# Devices where sequential assignment should use grouped blocks instead of interleaving.
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

PROTECTION_LAYOUT_DEVICES = {
    normalize_for_compare("Distance Protection"),
    normalize_for_compare("Control and Protection Panels"),
    normalize_for_compare("Transformer Protection"),
    normalize_for_compare("Line Overcurrent Protection"),
}

PROTECTION_LAYOUT_SPACING = 2.0

# Files that should be passed through without protection auto-layout or fill.
SKIP_BATCH_FILL_STEMS = {
    normalize_for_compare("connection points"),
    normalize_for_compare("connection_point"),
    normalize_for_compare("connection_points"),
    normalize_for_compare("connectionpoints"),
    normalize_for_compare("point connection"),
    normalize_for_compare("point connections"),
}

# Hard overrides for filename -> preferred match columns.
FILE_MATCH_OVERRIDES = {
    normalize_for_compare("BUSBAR1"): ["Substation ID", "SubstationID", "SUBSTATION NAMES"],
    normalize_for_compare("BUSBAR"): ["Substation ID", "SubstationID", "SUBSTATION NAMES"],
    normalize_for_compare("Cabin"): ["Substation ID", "SubstationID", "SUBSTATION NAMES"],
    normalize_for_compare("DISCONNECTOR SWITCHES1"): [
        "HV_Switch_ID",
        "HV Switch ID",
        "Composite_ID",
        "Composite ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("DISCONNECTOR SWITCH"): [
        "HV_Switch_ID",
        "HV Switch ID",
        "Composite_ID",
        "Composite ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("LIGHTNING ARRESTOR"): [
        "Lightining Arrester Name",
        "Lightning Arrester Name",
        "ArresterID",
        "Arrester Name",
    ],
    normalize_for_compare("HIGH VOLTAGE CIRCUIT BREAKER"): [
        "Circuit Breaker Name",
        "CircuitBreakerID",
        "CircuitBreaker_ID",
    ],
    normalize_for_compare("HIGH VOLTAGE CIRCUIT BREAKER.gpkg"): [
        "Circuit Breaker Name",
        "CircuitBreakerID",
        "CircuitBreaker_ID",
    ],
    normalize_for_compare("INDOR CB"): [
        "Circuit Breaker Name",
        "CircuitBreakerID",
        "CircuitBreaker_ID",
        "Circuit Breaker - Indoor SG ID",
        "Feeder Type",
    ],
    normalize_for_compare("LINE BAY"): [
        "LineBayID",
        "Line Bay ID",
        "Line_Bay_ID",
    ],
    normalize_for_compare("CURRENT TRANSFORMER"): [
        "Current Transformer Name",
        "CurrentTransformerID",
        "Current Transformer ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("INDOR CT"): [
        "Current Transformer Name",
        "CurrentTransfomerID",
        "Current Transformer ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
        "Feeder Type",
    ],
    normalize_for_compare("VOLTAGE TRANSFORMER"): [
        "Voltage Transformer Name",
        "VoltageTransfomer_ID",
        "Voltage Transformer ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("INDOR VT"): [
        "Voltage Transformer Name",
        "VoltageTransfomer_ID",
        "Voltage Transformer ID",
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
        "Feeder Type",
    ],
    normalize_for_compare("POWER_TRANSFORMER"): [
        "Transformer ID",
        "TransfomerID",
        "Transfomer_ID",
        "TransformerID",
    ],
    normalize_for_compare("SWITCHGEAR"): [
        "FeederID",
        "Feeder ID",
        "FeederName",
    ],
    normalize_for_compare("TRANS SYSTEM PROT1"): [
        "Line Bay ID",
        "LineBayID",
    ],
    normalize_for_compare("TRANS_SYSTEM PROT2"): [
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("TRANSFORMER"): [
        "Line Bay ID",
        "LineBayID",
        "Substation ID",
    ],
    normalize_for_compare("VOLTAGE TRANSFORMER"): [
        "Voltage Transformer Name",
        "VoltageTransfomer_ID",
        "Voltage Transformer ID",
        "Line Bay ID",
        "LineBayID",
    ],
    normalize_for_compare("POWER_TRANSFORMER"): [
        "Transformer ID",
        "TransfomerID",
        "TransformerID",
        "Transformer Id",
    ],
}


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

        norm_vals = [normalize_value_for_compare(v) for v in sample]
        norm_vals = [v for v in norm_vals if v]
        if not norm_vals:
            return 0.0

        alpha_flags = [any(ch.isalpha() for ch in v) for v in norm_vals]
        alpha_ratio = sum(alpha_flags) / len(alpha_flags) if alpha_flags else 0.0
        unique_count = len(set(norm_vals))

        lengths = [len(v) for v in norm_vals]
        median_len = statistics.median(lengths) if lengths else 0.0
        length_bonus = max(0.0, 10.0 - abs(median_len - 12.0))  # prefer reasonable name lengths

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

    candidates.sort(key=lambda x: (-x[0], -x[1], -x[2], len(normalize_for_compare(x[3]))))
    return candidates[0][3]


# =====================================================================
# DATAFRAME CLEANING
# =====================================================================

def _apply_global_forward_fill(df: pd.DataFrame) -> pd.DataFrame:
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


def forward_fill_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Forward-fill a specific column, treating blanks/whitespace as missing."""
    if df.empty or column not in df.columns:
        return df
    series = df[column].apply(strip_unicode_spaces)
    series = series.replace("", pd.NA)
    df[column] = series.ffill()
    return df


def clean_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(lambda c: c.map(lambda v: (pd.isna(v) if not isinstance(v, str) else not v.strip())))
    cleaned = df.loc[~mask.all(axis=1)].copy()
    cleaned.columns = df.columns
    cleaned = _apply_global_forward_fill(cleaned)
    return cleaned


def _detect_header_row(raw_df: pd.DataFrame) -> int:
    """
    Identify which row contains headers. Looks for cells mentioning 'substation'
    and picks the earliest row with the strongest signal.
    """
    best_row = 0
    best_score = -1
    for idx, row in raw_df.head(10).iterrows():  # scan first few rows only
        cleaned_cells = [_clean_column_name(c) for c in row]
        substation_hits = sum("substation" in normalize_for_compare(c) for c in cleaned_cells if isinstance(c, str))
        non_empty = sum(bool(str(c).strip()) for c in cleaned_cells)
        score = substation_hits * 10 + min(non_empty, 5)  # prioritize substation mentions; small tie-break on density
        if score > best_score:
            best_score = score
            best_row = idx
    return best_row


# =====================================================================
# GPKG CLEANING
# =====================================================================

def ensure_valid_gpkg_dtypes(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64tz_dtype(series):
        series = series.dt.tz_localize(None)
    elif pd.api.types.is_timedelta64_dtype(series):
        series = series.astype(str)

    if pd.api.types.is_numeric_dtype(series):
        if pd.api.types.is_integer_dtype(series):
            return series.astype("Int64")
        return series.astype("float64")

    if pd.api.types.is_object_dtype(series) or any(
        isinstance(v, (list, dict, set, tuple)) for v in series.dropna().head(5)
    ):
        series = series.apply(lambda v: str(v) if v is not None else None)

    return series


def sanitize_gdf_for_gpkg(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    geometry_name = out.geometry.name

    new_cols = []
    for col in out.columns:
        if col == geometry_name:
            new_cols.append(col)
            continue
        c = _clean_column_name(col)
        if len(c) > MAX_GPKG_NAME_LENGTH:
            c = c[:MAX_GPKG_NAME_LENGTH]
        new_cols.append(c)

    # Ensure cleaned names stay unique; duplicate labels make pandas return DataFrames
    # for column selection, which then triggers ambiguous truth-value errors downstream.
    out.columns = ensure_unique_columns(new_cols)

    for col in out.columns:
        if col == geometry_name:
            continue
        series = out[col]
        # Defensive: if duplicate column names slipped through, take the first match.
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        series = ensure_valid_gpkg_dtypes(series)
        mask = pd.isna(series)
        if bool(mask.any()) and not pd.api.types.is_numeric_dtype(series):
            series = series.astype(object)
            series[mask] = None
        out[col] = series

    return out


def st_dataframe_safe(df, rows: int | None = None):
    """Render dataframes safely in Streamlit by stringifying geometry columns to avoid Arrow errors."""
    try:
        preview = df.head(rows) if rows else df
        if hasattr(preview, "geometry"):
            preview = preview.copy()
            geom_col = preview.geometry.name
            preview[geom_col] = preview[geom_col].apply(lambda g: getattr(g, "wkt", None) if g is not None else None)
        elif "geometry" in preview.columns:
            preview = preview.copy()
            preview["geometry"] = preview["geometry"].apply(lambda g: getattr(g, "wkt", None) if hasattr(g, "wkt") else str(g))
        st.dataframe(preview)
    except Exception:
        st.dataframe(df)


# =====================================================================
# MERGE LOGIC
# =====================================================================

def merge_without_duplicates(gdf, df, left_key, right_key):
    """
    Join df onto gdf with Excel values overwriting GeoPackage values when matched.
    Uses normalized key lookup instead of pandas merge to avoid ambiguous truthiness
    and to better control column handling.
    """
    base = gdf.copy()
    incoming = df.copy()

    geometry_name = base.geometry.name if hasattr(base, "geometry") else None

    # Clean and uniquify incoming column names
    incoming.columns = ensure_unique_columns([_clean_column_name(c) for c in incoming.columns])

    # Detect collisions
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

    # Normalized join keys
    base_norm = base[left_key].map(normalize_value_for_compare)
    incoming_norm = incoming[right_key].map(normalize_value_for_compare)
    incoming[nk := "_norm_key"] = incoming_norm

    # Build lookup dicts for incoming columns keyed by normalized join key
    incoming_dicts = {col: incoming.set_index(nk)[col].to_dict() for col in incoming.columns if col != nk}

    # Map normalized incoming columns to existing GPKG columns (by normalized name)
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

    # Apply incoming values
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


# Manual mapping of GPKG/file names to exact sheet names.
GPKG_SHEET_MAP: dict[str, list[str]] = {
    normalize_for_compare("48VDC BATTERY"): ["48VDC BATTERY"],
    normalize_for_compare("48VDC CHARGER"): ["48VDC CHARGER"],
    normalize_for_compare("110VDC BATTERY"): ["110VDC BATTERY"],
    normalize_for_compare("110VDC CHARGER"): ["110VDC CHARGER"],
    normalize_for_compare("BUSBAR"): ["BUSBAR"],
    normalize_for_compare("CABIN"): ["SUBSTATION"],
    normalize_for_compare("CB INDOR SWITCHGEAR"): ["CB- INDR STCH G- 30,15KV"],
    normalize_for_compare("CT INDOR SWITCHGEAR"): ["CT INDR STCH G - 30,15KV"],
    normalize_for_compare("CURRENT TRANSFORMER"): ["CURRENT TRANSFORMER"],
    normalize_for_compare("DIGITAL FAULT RECORDER"): ["DIGITAL FAULT RECORDER"],
    normalize_for_compare("DISCONNECTOR SWITCH"): ["DISCONNECTOR SWITCH"],
    normalize_for_compare("HIGH_VOLTAGE_CIRCUIT_BREAKER"): ["HIGH VOLTAGE CIRCUIT BREAKER"],
    normalize_for_compare("INDOR SWITCHGEAR TABLE"): ["INDOR SWITCH GEAR TABLE"],
    normalize_for_compare("LIGHTNING ARRESTOR"): ["LIGHTINING ARRESTERS"],
    normalize_for_compare("LINE BAY"): ["LINE BAYS"],
    normalize_for_compare("POWER CABLE TO TRANSFORMER"): ["POWER CABLE TO TRANSFORMER"],
    normalize_for_compare("TELECOM"): ["TELECOM SDH", "TELECOM ODF"],
    normalize_for_compare("TRANS_SYSTEM PROT1"): ["TRANS- SYSTEM PROT1"],
    normalize_for_compare("TRANSFORMERS"): ["TRANSFORMER 2"],
    normalize_for_compare("UPS"): ["UPS"],
    normalize_for_compare("VOLTAGE TRANSFORMER"): ["VOLTAGE TRANSFORMER"],
    normalize_for_compare("VT INDOR SWITCHGEAR"): ["VT INDR STCH G - 30,15KV"],
}


def detect_best_sheet(excel_file: pd.ExcelFile, gdf_columns: list[str]) -> str | None:
    """
    Pick the Excel sheet whose cleaned header best matches the GeoPackage columns.
    Uses normalized header overlap; returns None if no sheets found.
    """
    best_sheet = None
    best_score = 0.0
    gdf_norm = {normalize_for_compare(c) for c in gdf_columns}
    for sheet in excel_file.sheet_names:
        header = _get_sheet_header(excel_file, sheet)
        if not header:
            continue
        header_norm = {normalize_for_compare(h) for h in header if h}
        overlap = len(gdf_norm & header_norm)
        denom = max(len(header_norm), 1)
        score = overlap / denom
        if score > best_score:
            best_score = score
            best_sheet = sheet
    return best_sheet


def select_sheet_for_gpkg(
    excel_file: pd.ExcelFile, gpkg_name: str, gdf_columns: list[str], auto_sheet: bool, fallback_sheet: str
) -> str:
    """
    Choose the sheet for a given GeoPackage name using the manual map first,
    then optional auto-selection, then fallback. If a mapping exists but is not
    present in this workbook, returns None to allow trying another workbook.
    """
    norm = normalize_for_compare(Path(gpkg_name).stem)

    # Build normalized lookup for sheet names in this workbook
    sheet_lookup = {normalize_for_compare(s): s for s in excel_file.sheet_names}

    candidates = GPKG_SHEET_MAP.get(norm, [])
    if candidates:
        for cand in candidates:
            cand_norm = normalize_for_compare(cand)
            if cand_norm in sheet_lookup:
                return sheet_lookup[cand_norm]
        return None  # mapped sheet not present in this workbook

    if auto_sheet:
        detected = detect_best_sheet(excel_file, gdf_columns)
        if detected:
            return detected
    return fallback_sheet


def detect_join_columns(
    left_df: pd.DataFrame, right_df: pd.DataFrame, geometry_name: str | None = None
) -> tuple[str | None, str | None, int]:
    """
    Heuristic to find join columns between GeoPackage dataframe and Excel dataframe.
    Prefers value overlap (intersection count), falls back to column-name similarity.
    Returns left_key, right_key, and the number of matching keys found.
    """

    def _norm_series(series: pd.Series) -> pd.Series:
        return series.dropna().map(normalize_value_for_compare)

    left_candidates = [c for c in left_df.columns if c != geometry_name]
    right_candidates = list(right_df.columns)

    best = (None, None, 0, 0.0)  # left, right, intersection_count, coverage
    for lc in left_candidates:
        left_norm = set(_norm_series(left_df[lc]))
        if not left_norm:
            continue
        for rc in right_candidates:
            right_norm = set(_norm_series(right_df[rc]))
            if not right_norm:
                continue
            inter = len(left_norm & right_norm)
            coverage = inter / max(len(right_norm), 1)
            if inter > best[2] or (inter == best[2] and coverage > best[3]):
                best = (lc, rc, inter, coverage)

    left_key, right_key, match_count, coverage = best
    if match_count > 0:
        return left_key, right_key, match_count

    # fallback: header similarity
    best = (None, None, 0.0)
    for lc in left_candidates:
        norm_l = normalize_for_compare(lc)
        for rc in right_candidates:
            norm_r = normalize_for_compare(rc)
            ratio = difflib.SequenceMatcher(None, norm_l, norm_r).ratio()
            if ratio > best[2]:
                best = (lc, rc, ratio)
    if best[2] >= 0.6:
        return best[0], best[1], 0
    return None, None, 0


def preferred_match_columns(device_name: str) -> list[str]:
    """Return preferred match columns for specific devices when row-matching supervisor data."""
    norm = normalize_for_compare(device_name)
    preferences = {
        normalize_for_compare("Line Bay"): [
            "LineBayID",
            "Line Bay ID",
            "Line_Bay_ID",
            "Line Bay Name",
            "Line_Bay_Name",
        ],
        normalize_for_compare("MV Switch gear"): [
            "FeederID",
            "Feeder ID",
            "FeederName",
            "Feeder Name",
        ],
        normalize_for_compare("Lightning Arrester"): [
            "Lightining Arrester Name",
            "Lightning Arrester Name",
            "ArresterID",
            "Arrester Name",
            "Arrester ID",
        ],
        normalize_for_compare("High Voltage Circuit Breaker/High Voltage Circuit Breaker"): [
            "Circuit Breaker Name",
            "CircuitBreakerID",
            "CircuitBreaker_ID",
        ],
        normalize_for_compare("High Voltage Switch/High Voltage Switch"): [
            "HV_Switch_ID",
            "HV Switch ID",
            "Composite_ID",
            "Composite ID",
            "Composite",
        ],
        normalize_for_compare("High Voltage Busbar/Medium Voltage Busbar"): [
            "Substation ID",
            "SubstationID",
            "SUBSTATION NAMES",
        ],
        normalize_for_compare("Substation/Cabin"): [
            "Substation ID",
            "SubstationID",
            "SUBSTATION NAMES",
        ],
        normalize_for_compare("Earthing Transformer"): [
            "transfomerID",
            "TransformerID",
            "Transformer ID",
            "transfomer ID",
        ],
    }
    return preferences.get(norm, [])


def match_overrides_for_file(file_name: str) -> list[str]:
    norm = normalize_for_compare(Path(file_name).stem)
    return FILE_MATCH_OVERRIDES.get(norm, [])


def derive_layer_name_from_filename(name: str) -> str:
    base = Path(name).stem.strip() or "dataset"
    base = base.replace(" ", "_").lower()
    if len(base) > MAX_GPKG_NAME_LENGTH:
        base = base[:MAX_GPKG_NAME_LENGTH]
    return base


def run_app() -> None:
    """Streamlit entrypoint."""
    st.set_page_config(page_title="Internal Substation Attribute Loader", layout="wide")

    st.title("Internal Substation Attribute Loader")
    st.caption("Use the internal master workbook to populate attributes for a single substation.")

    # Select workbook
    workbooks = list_reference_workbooks()
    if not workbooks:
        st.error("No reference workbooks found in reference_data.")
        st.stop()

    labels = list(workbooks.keys())
    default_idx = 0
    for pref in WORKBOOK_PRIORITY:
        if pref in labels:
            default_idx = labels.index(pref)
            break

    selected_label = st.selectbox("Select Reference Workbook", labels, index=default_idx)
    workbook_path = workbooks[selected_label]

    st.info(f"Using workbook: **{selected_label}**")

    # Upload GPKG
    gpkg_file = st.file_uploader("Upload GeoPackage (.gpkg)", type=["gpkg"])
    if gpkg_file is None:
        st.stop()

    try:
        gdf = gpd.read_file(gpkg_file)
    except Exception as e:
        st.error(f"Failed to read GPKG: {e}")
        st.stop()

    st.subheader("GeoPackage Preview")
    st.write(f"Features: **{len(gdf):,}**")
    st_dataframe_safe(gdf, PREVIEW_ROWS)

    # Select sheet
    excel_file = get_excel_file(workbook_path)
    sheet = st.selectbox("Select Equipment Type (Excel Sheet)", excel_file.sheet_names)
    if not sheet:
        st.stop()

    try:
        raw_df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str, header=None)
        header_row = _detect_header_row(raw_df)
        header = [_clean_column_name(c) for c in raw_df.iloc[header_row]]
        header = ensure_unique_columns(header)
        df = raw_df.iloc[header_row + 1 :].copy()
        df.columns = header
        df.reset_index(drop=True, inplace=True)
        df = _apply_global_forward_fill(df)
        df = clean_empty_rows(df)
    except Exception as e:
        st.error(f"Error loading sheet {sheet}: {e}")
        st.stop()

    # Detect substation column
    sub_col = detect_substation_column(df)

    st.subheader("Substation Selection")

    if sub_col is None:
        sub_col = st.selectbox("Select Substation Column", df.columns)
        st.warning("Auto-detection failed - manual selection required.")
    else:
        st.success(f"Detected Substation Column: **{sub_col}**")

    # Ensure merged/blank substation cells propagate to following rows
    df = forward_fill_column(df, sub_col)
    # Extract substations
    raw_subs = df[sub_col].dropna().map(lambda x: str(x))
    # Remove invisible/bom spaces but keep normal ASCII spaces
    def _clean_sub_value(val: str) -> str:
        for ch in INVISIBLE_HEADER_CHARS:
            val = val.replace(ch, "")
        return val.strip()

    raw_subs = raw_subs.map(_clean_sub_value).replace("", pd.NA).dropna()
    # Build mapping of normalized -> representative label
    norm_to_label = {}
    for val in raw_subs:
        norm = normalize_value_for_compare(val)
        if norm and norm not in norm_to_label:
            norm_to_label[norm] = val

    substations = sorted(norm_to_label.values())

    if not substations:
        st.error("No substation names found. Check the Excel formatting.")
        st.stop()

    selected_sub = st.selectbox("Choose Substation", substations)

    # Filter rows
    norm_selected = normalize_value_for_compare(selected_sub)
    norm_col = df[sub_col].map(normalize_value_for_compare)
    filter_mask = (norm_col == norm_selected).fillna(False)
    filtered_df = df.loc[filter_mask].copy()

    st.write(f"Filtered rows: **{len(filtered_df)}**")
    st_dataframe_safe(filtered_df, PREVIEW_ROWS)

    # Join fields
    st.subheader("Join Fields")
    left_key = st.selectbox("Field in GeoPackage (left key)", gdf.columns)
    right_key = st.selectbox("Field in Excel sheet (right key)", filtered_df.columns)

    # Merge button
    if st.button("Merge and Prepare Updated GeoPackage"):
        try:
            merged = merge_without_duplicates(gdf, filtered_df, left_key, right_key)
            st.success("Merge successful!")
            st_dataframe_safe(merged, PREVIEW_ROWS)

            # Save temp file
            layer_name = derive_layer_name_from_filename(gpkg_file.name)

            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                temp_path = tmp.name

            safe = sanitize_gdf_for_gpkg(merged)
            safe.to_file(temp_path, driver="GPKG", layer=layer_name)

            with open(temp_path, "rb") as f:
                data = f.read()

            download_name = gpkg_file.name
            st.download_button(
                "Download Updated GeoPackage",
                data=data,
                file_name=download_name,
                mime="application/geopackage+sqlite3",
            )

        except Exception as e:
            st.error(f"Merge failed: {e}")

    # =====================================================================
    # AUTOMATED BATCH LOADER (ZIP)
    # =====================================================================
    st.markdown("---")
    st.header("Automated Batch Loader")
    st.caption(
        "Upload a ZIP containing GeoPackages named by substation. The app will auto-pick the sheet, substation, join fields, and return merged GeoPackages."
    )

    batch_zip = st.file_uploader("Upload ZIP of GeoPackages", type=["zip"], key="batch_zip")
    auto_sheet = st.checkbox("Auto-select equipment sheet per GeoPackage", value=True, key="batch_auto_sheet")
    default_sheet_idx = excel_file.sheet_names.index(sheet) if sheet in excel_file.sheet_names else 0
    fallback_sheet = st.selectbox(
        "Fallback sheet (used if auto selection fails)",
        excel_file.sheet_names,
        index=default_sheet_idx,
        key="batch_fallback_sheet",
    )

    if batch_zip is not None and st.button("Run Automated Batch Merge"):
        tmp_in_dir = Path(tempfile.mkdtemp())
        tmp_out_dir = Path(tempfile.mkdtemp())
        log_lines = []
        try:
            zip_path = tmp_in_dir / "input.zip"
            with open(zip_path, "wb") as f:
                f.write(batch_zip.getbuffer())
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmp_in_dir)

            gpkg_paths = list(tmp_in_dir.rglob("*.gpkg"))
            if not gpkg_paths:
                st.error("No GeoPackages found inside the ZIP.")
            else:
                ref_wbs = list_reference_workbooks()
                # Prioritize the user-selected workbook, then others.
                ordered_refs: list[tuple[str, Path]] = []
                if selected_label in ref_wbs:
                    ordered_refs.append((selected_label, ref_wbs.pop(selected_label)))
                ordered_refs.extend(sorted(ref_wbs.items(), key=lambda x: x[0]))

                for gpkg_path in sorted(gpkg_paths):
                    try:
                        # Substation name is taken from the top-level folder in the ZIP; fallback to file stem.
                        rel_parts = gpkg_path.relative_to(tmp_in_dir).parts
                        substation_candidates = []
                        if len(rel_parts) > 1:
                            substation_candidates.append(rel_parts[0])
                        substation_candidates.append(gpkg_path.stem)
                        layers = list_gpkg_layers(gpkg_path)
                        layer_name = layers[0] if layers else None
                        gdf_in = gpd.read_file(gpkg_path, layer=layer_name) if layer_name else gpd.read_file(gpkg_path)

                        merged_ok = False

                        for wb_label, wb_path in ordered_refs:
                            try:
                                excel_file = get_excel_file(wb_path)
                                fb_sheet = fallback_sheet if fallback_sheet in excel_file.sheet_names else excel_file.sheet_names[0]
                                # Choose sheet using mapping -> auto-detect -> fallback
                                chosen_sheet = select_sheet_for_gpkg(
                                    excel_file, gpkg_path.name, list(gdf_in.columns), auto_sheet, fb_sheet
                                )
                                if chosen_sheet is None or chosen_sheet not in excel_file.sheet_names:
                                    continue

                                df_sheet = load_reference_sheet(wb_path, chosen_sheet)
                                cache_sub_key = (_excel_key_from_file(excel_file), chosen_sheet)
                                sub_col_auto = _SUB_COL_CACHE.get(cache_sub_key)
                                if sub_col_auto is None:
                                    sub_col_auto = detect_substation_column(df_sheet)
                                    _SUB_COL_CACHE[cache_sub_key] = sub_col_auto
                                if sub_col_auto is None:
                                    continue
                                df_sheet = forward_fill_column(df_sheet, sub_col_auto)

                                norm_col = df_sheet[sub_col_auto].map(normalize_value_for_compare)
                                filtered_df = pd.DataFrame()
                                for substation_name in substation_candidates:
                                    target_norm = normalize_value_for_compare(substation_name)
                                    filtered_df = df_sheet.loc[(norm_col == target_norm).fillna(False)].copy()
                                    if not filtered_df.empty:
                                        break
                                if filtered_df.empty:
                                    continue

                                geometry_name = gdf_in.geometry.name if hasattr(gdf_in, "geometry") else None
                                left_key, right_key, match_count = detect_join_columns(
                                    gdf_in, filtered_df, geometry_name=geometry_name
                                )
                                if left_key is None or right_key is None:
                                    # fallback to substation column matching if present in gdf
                                    guess_left = detect_substation_column(gdf_in)
                                    if guess_left and guess_left in gdf_in.columns:
                                        left_key = left_key or guess_left
                                    right_key = right_key or sub_col_auto
                                    match_count = 0
                                if left_key is None or right_key is None:
                                    continue

                                merged = merge_without_duplicates(gdf_in, filtered_df, left_key, right_key)
                                safe = sanitize_gdf_for_gpkg(merged)
                                out_layer = layer_name or derive_layer_name_from_filename(gpkg_path.name)
                                out_path = tmp_out_dir / gpkg_path.name
                                safe.to_file(out_path, driver="GPKG", layer=out_layer)
                                log_lines.append(
                                    f"{gpkg_path.name}: merged using workbook '{wb_label}', sheet '{chosen_sheet}' on {left_key} -> {right_key} (matches: {match_count})."
                                )
                                merged_ok = True
                                break
                            except Exception:
                                continue

                        if not merged_ok:
                            log_lines.append(f"{gpkg_path.name}: skipped (no rows found for substation '{substation_name}' in any workbook).")
                    except Exception as exc:
                        log_lines.append(f"{gpkg_path.name}: failed ({exc}).")

                if list(tmp_out_dir.glob("*.gpkg")):
                    zip_out = shutil.make_archive(str(tmp_out_dir / "merged"), "zip", root_dir=tmp_out_dir, base_dir=".")
                    with open(zip_out, "rb") as f:
                        data = f.read()
                    st.download_button(
                        "Download Merged GeoPackages (zip)",
                        data=data,
                        file_name="merged_geopackages.zip",
                        mime="application/zip",
                    )
                st.text_area("Batch log", value="\n".join(log_lines) if log_lines else "No logs.", height=200)
        finally:
            shutil.rmtree(tmp_in_dir, ignore_errors=True)
            shutil.rmtree(tmp_out_dir, ignore_errors=True)

    # =====================================================================
    # SCHEMA MAPPING FOR EQUIPMENT GPKG
    # =====================================================================
    st.header("Schema Mapping: Equipment GPKG to Electric Device Fields")
    st.caption(
        "Upload an equipment GeoPackage, pick a layer and a schema sheet, review/adjust the suggested column mapping, and download an updated GPKG with standardized fields."
    )

    source_type = st.selectbox("Equipment data source", ["GeoPackage (gpkg)", "FileGDB (gdb/zip)"], index=0, key="map_source")
    map_file = None
    if source_type.startswith("GeoPackage"):
        map_file = st.file_uploader("Upload Equipment GeoPackage for Schema Mapping", type=["gpkg"], key="map_gpkg")
    else:
        map_file = st.file_uploader("Upload Equipment FileGDB for Schema Mapping (zip the .gdb folder)", type=["gdb", "zip"], key="map_gdb")

    st.markdown("---")
    st.header("Supervisor Device Sheet Filler")
    st.caption(
        "Upload a device GeoPackage and a supervisor Electric-device workbook; choose a device entry and fill its attributes into the GPKG with proper data types."
    )
    sup_gpkg_files = st.file_uploader(
        "Upload device GeoPackage (GPKG)", type=["gpkg"], accept_multiple_files=True, key="sup_gpkg"
    )
    sup_wb_files = list_supervisor_workbooks()
    sup_wb_path = None
    if sup_wb_files:
        st.caption(f"Supervisor workbooks folder: {SUPERVISOR_WORKBOOK_DIR}")
        sup_wb_label = st.selectbox("Supervisor workbook (Electric device format)", list(sup_wb_files.keys()), key="sup_wb_select")
        sup_wb_path = sup_wb_files[sup_wb_label]
    else:
        st.info(f"Add supervisor workbooks to: {SUPERVISOR_WORKBOOK_DIR}")

    if sup_gpkg_files and sup_wb_path:
        try:
            sup_excel = pd.ExcelFile(sup_wb_path)
            sup_sheet = st.selectbox("Supervisor sheet", sup_excel.sheet_names, key="sup_sheet")
            raw_sup = pd.read_excel(sup_wb_path, sheet_name=sup_sheet, dtype=str, header=None)
            raw_sup.iloc[:, 0] = raw_sup.iloc[:, 0].ffill()
            device_options = sorted(set(raw_sup.iloc[:, 0].dropna().astype(str))) if not raw_sup.empty else []
            device_choice = st.selectbox("Device entry", device_options, key="sup_device")
            equip_map_sup = load_gpkg_equipment_map()
            protection_in_uploads = False
            ups_upload_candidate = None
            line_bay_upload_candidate = None
            if sup_gpkg_files:
                for file_obj in sup_gpkg_files:
                    try:
                        dev_name = resolve_equipment_name(file_obj.name, device_options, equip_map_sup)
                    except Exception:
                        continue
                    if normalize_for_compare(dev_name) in PROTECTION_LAYOUT_DEVICES:
                        protection_in_uploads = True
                    if (
                        ups_upload_candidate is None
                        and normalize_for_compare(dev_name) == normalize_for_compare("Uninterruptable power supply(UPS)")
                    ):
                        ups_upload_candidate = file_obj
                    if (
                        line_bay_upload_candidate is None
                        and normalize_for_compare(dev_name) == normalize_for_compare("Line Bay")
                    ):
                        line_bay_upload_candidate = file_obj
                if ups_upload_candidate is None:
                    for file_obj in sup_gpkg_files:
                        if "ups" in normalize_for_compare(Path(file_obj.name).stem):
                            ups_upload_candidate = file_obj
                            break
                if line_bay_upload_candidate is None:
                    for file_obj in sup_gpkg_files:
                        stem_norm = normalize_for_compare(Path(file_obj.name).stem)
                        if "linebay" in stem_norm or "line bay" in stem_norm or "line_bay" in stem_norm:
                            line_bay_upload_candidate = file_obj
                            break
            line_bay_info = None
            show_line_bay = (
                normalize_for_compare(device_choice) in LINE_BAY_SPATIAL_DEVICES
                or line_bay_upload_candidate is not None
            )
            with st.expander("Line Bay polygons for High Voltage Line snapping", expanded=show_line_bay):
                line_bay_path = None
                line_bay_layer = None
                line_bay_label = None
                line_bay_gpkg = st.file_uploader(
                    "Optional Line Bay polygons (GPKG)",
                    type=["gpkg"],
                    key="sup_line_bay_gpkg",
                )
                if line_bay_gpkg is not None:
                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmplb:
                        tmplb.write(line_bay_gpkg.getbuffer())
                        line_bay_path = Path(tmplb.name)
                    line_bay_label = line_bay_gpkg.name
                elif line_bay_upload_candidate is not None:
                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmplb:
                        tmplb.write(line_bay_upload_candidate.getbuffer())
                        line_bay_path = Path(tmplb.name)
                    line_bay_label = line_bay_upload_candidate.name
                if line_bay_path is not None:
                    line_bay_layers = list_gpkg_layers(line_bay_path)
                    if not line_bay_layers:
                        st.warning("No layers found in Line Bay GeoPackage.")
                    else:
                        layer_label = "Line Bay layer"
                        if line_bay_label:
                            st.caption(f"Using Line Bay source: {line_bay_label}")
                        line_bay_layer = st.selectbox(layer_label, line_bay_layers, key="sup_line_bay_layer")
                        try:
                            gdf_bay_preview = gpd.read_file(line_bay_path, layer=line_bay_layer)
                            geom_col = gdf_bay_preview.geometry.name if hasattr(gdf_bay_preview, "geometry") else None
                            candidate_cols = [c for c in gdf_bay_preview.columns if c != geom_col]
                            if candidate_cols:
                                def _score_bay_col(col: str) -> int:
                                    norm = normalize_for_compare(col)
                                    score = 0
                                    for kw in ["name", "line", "bay", "id"]:
                                        if kw in norm:
                                            score += 1
                                    return score
                                default_col = sorted(candidate_cols, key=lambda c: (-_score_bay_col(c), len(c)))[0]
                                line_bay_field = st.selectbox(
                                    "Line Bay name field",
                                    candidate_cols,
                                    index=candidate_cols.index(default_col),
                                    key="sup_line_bay_field",
                                )
                                use_line_bay_match = st.checkbox(
                                    "Use Line Bay polygons for High Voltage Line snapping/matching",
                                    value=True,
                                    key="sup_line_bay_use",
                                )
                                if use_line_bay_match:
                                    line_bay_info = {
                                        "path": line_bay_path,
                                        "layer": line_bay_layer,
                                        "field": line_bay_field,
                                    }
                            else:
                                st.warning("No attribute columns found in Line Bay layer.")
                        except Exception:
                            st.warning("Could not read Line Bay layer to select a name field.")
            ups_anchor_info = None
            show_protection_layout = (
                normalize_for_compare(device_choice) in PROTECTION_LAYOUT_DEVICES
                or protection_in_uploads
                or ups_upload_candidate is not None
            )
            with st.expander("Protection auto-create from UPS", expanded=show_protection_layout):
                ups_path = None
                ups_layer = None
                ups_label = None
                ups_gpkg = st.file_uploader(
                    "Optional UPS GeoPackage (GPKG) for protection layout",
                    type=["gpkg"],
                    key="sup_ups_gpkg",
                )
                if ups_gpkg is not None:
                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpups:
                        tmpups.write(ups_gpkg.getbuffer())
                        ups_path = Path(tmpups.name)
                    ups_label = ups_gpkg.name
                    ups_layers = list_gpkg_layers(ups_path)
                    if not ups_layers:
                        st.warning("No layers found in UPS GeoPackage.")
                    else:
                        ups_layer = st.selectbox("UPS layer", ups_layers, key="sup_ups_layer")
                elif ups_upload_candidate is not None:
                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpups:
                        tmpups.write(ups_upload_candidate.getbuffer())
                        ups_path = Path(tmpups.name)
                    ups_label = ups_upload_candidate.name
                    ups_layers = list_gpkg_layers(ups_path)
                    if not ups_layers:
                        st.warning("No layers found in UPS GeoPackage from uploads.")
                    else:
                        ups_layer = st.selectbox(
                            "UPS layer (from uploaded GPKGs)", ups_layers, key="sup_ups_layer_auto"
                        )
                if ups_path and ups_layer:
                    if ups_label:
                        st.caption(f"Using UPS source: {ups_label}")
                    spacing_val = st.number_input(
                        "Protection layout spacing (map units)",
                        min_value=0.1,
                        value=float(PROTECTION_LAYOUT_SPACING),
                        step=0.1,
                        key="sup_protection_spacing",
                    )
                    use_layout = st.checkbox(
                        "Place protection devices below UPS",
                        value=True,
                        key="sup_protection_layout",
                    )
                    if use_layout:
                        ups_anchor_info = {
                            "path": ups_path,
                            "layer": ups_layer,
                            "spacing": float(spacing_val),
                        }
                elif ups_upload_candidate is None:
                    st.info("Upload an UPS GeoPackage or include UPS among uploads to place protection devices.")
            device_instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, device_choice)
            instance_labels = [inst["label"] for inst in device_instances]
            selected_instance = None
            if instance_labels:
                inst_label = st.selectbox("Device instance", instance_labels, key="sup_device_instance")
                selected_instance = next((i for i in device_instances if i["label"] == inst_label), None)
            else:
                st.warning("No instances found for this device in the supervisor sheet.")
            fill_mode_options = [
                "Single layer (apply chosen instance to all rows)",
                "Match rows to instances (single GPKG)",
                "One GeoPackage per instance",
            ]
            if instance_labels and len(device_instances) > 1:
                default_mode_idx = 1  # match rows by default when multiple instances exist
                fill_mode = st.radio("Fill mode", fill_mode_options, index=default_mode_idx, key="sup_fill_mode")
            else:
                fill_mode = fill_mode_options[0]

                # UI flag: whether to distribute parsed supervisor instances across features when no matches found
                seq_assign_fallback = st.checkbox(
                    "Distribute parsed supervisor instances across features when no matches are found",
                    value=True,
                    key="sup_seq_assign",
                )

            def _tokenize(text: str) -> set[str]:
                return set(
                    t.lower()
                    for t in re.findall(r"[A-Za-z][a-z]+|[A-Za-z]+|[0-9]+", text.replace("_", " "))
                    if t
                )

            def choose_target_column(field_name: str, existing_columns: list[str], norm_lookup: dict[str, str]) -> str:
                import difflib

                norm_field = normalize_for_compare(field_name)
                if norm_field in norm_lookup:
                    return norm_lookup[norm_field]
                tokens_field = _tokenize(field_name)
                best_col = None
                best_score = 0.0
                for col in existing_columns:
                    tokens_col = _tokenize(str(col))
                    token_overlap = len(tokens_field & tokens_col) / max(len(tokens_field), 1)
                    sim = difflib.SequenceMatcher(None, norm_field, normalize_for_compare(col)).ratio()
                    score = 0.6 * token_overlap + 0.4 * sim
                    if score > best_score:
                        best_score = score
                        best_col = col
                if best_score >= 0.55 and best_col is not None:
                    return best_col
                return field_name

            def fill_one_gpkg(
                file_obj,
                device_name: str,
                layer_override: str | None = None,
                field_map: dict[str, Any] | None = None,
                match_column: str | None = None,
                instance_map: dict[str, tuple[dict[str, Any], list[str]]] | None = None,
                default_fields: dict[str, Any] | None = None,
                field_order: list[str] | None = None,
                sequential_instances: list[dict[str, Any]] | None = None,
                line_bay_info: dict[str, Any] | None = None,
                ups_anchor_info: dict[str, Any] | None = None,
            ) -> tuple[Path, str]:
                # normalize sequential_instances to a list of entries with fields + optional ids
                seq_entries: list[dict[str, Any]] = []
                if sequential_instances:
                    for inst in sequential_instances:
                        if isinstance(inst, dict) and "fields" in inst:
                            seq_entries.append(
                                {
                                    "fields": inst.get("fields", {}) or {},
                                    "id": inst.get("id_value"),
                                    "name": inst.get("name_value"),
                                }
                            )
                        else:
                            seq_entries.append({"fields": inst if isinstance(inst, dict) else {}, "id": None, "name": None})

                block_assign = normalize_for_compare(device_name) in BLOCK_ASSIGN_DEVICES

                def _build_seq_entry_order(total_rows: int, total_entries: int) -> list[int]:
                    if total_rows <= 0 or total_entries <= 0:
                        return []
                    if not block_assign or total_entries == 1:
                        return [i % total_entries for i in range(total_rows)]
                    base = total_rows // total_entries
                    remainder = total_rows % total_entries
                    order: list[int] = []
                    for entry_idx in range(total_entries):
                        size = base + (1 if entry_idx < remainder else 0)
                        if size <= 0:
                            continue
                        order.extend([entry_idx] * size)
                    if len(order) < total_rows:
                        order.extend([total_entries - 1] * (total_rows - len(order)))
                    return order[:total_rows]

                def _pick_seq_entry_by_feeder(
                    row_idx: int,
                    row_rank: int,
                    gdf_local: gpd.GeoDataFrame,
                    seq_order: list[int],
                    group_map: dict[int, int] | None,
                    prefix_map: dict[int, dict[str, Any]] | None,
                ) -> dict[str, Any]:
                    """Choose sequential instance based on feeder type if available, else follow ordered groups."""
                    if not seq_entries:
                        return {}
                    feeder_col = None
                    norm_lookup = {normalize_for_compare(c): c for c in gdf_local.columns}
                    for cand in ["feeder type", "feeder_type", "feeder category"]:
                        if normalize_for_compare(cand) in norm_lookup:
                            feeder_col = norm_lookup[normalize_for_compare(cand)]
                            break
                    if feeder_col:
                        try:
                            val = gdf_local.loc[row_idx, feeder_col]
                        except Exception:
                            val = gdf_local.iloc[row_rank][feeder_col] if row_rank < len(gdf_local) else None
                        norm_val = normalize_value_for_compare(val)
                        def _match_entry(target: str) -> dict[str, Any] | None:
                            for ent in seq_entries:
                                ident = ent.get("id") or ent.get("name")
                                ident_norm = normalize_for_compare(ident)
                                if target in ident_norm:
                                    return ent
                            return None
                        if "line" in norm_val:
                            chosen = _match_entry("mv3") or _match_entry("3")
                            if chosen:
                                return chosen
                        if "transformer" in norm_val:
                            chosen = _match_entry("mv1") or _match_entry("1")
                            if chosen:
                                return chosen
                    if prefix_map and row_idx in prefix_map:
                        return prefix_map[row_idx]
                    if group_map and row_idx in group_map:
                        group_idx = group_map[row_idx]
                        if 0 <= group_idx < len(seq_entries):
                            return seq_entries[group_idx]
                    if seq_order and row_rank < len(seq_order):
                        return seq_entries[seq_order[row_rank]]
                    return seq_entries[row_rank % len(seq_entries)]
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                    tmp.write(file_obj.getbuffer())
                    gpkg_path = Path(tmp.name)
                layers = list_gpkg_layers(gpkg_path)
                layer = layer_override or (layers[0] if layers else None)
                if not layer:
                    raise ValueError("No layers found in the uploaded GeoPackage.")
                gdf_sup_local = gpd.read_file(gpkg_path, layer=layer)
                geom_name = gdf_sup_local.geometry.name if hasattr(gdf_sup_local, "geometry") else None
                geom_crs = gdf_sup_local.crs if hasattr(gdf_sup_local, "crs") else None
                layout_applied = False
                if (
                    ups_anchor_info
                    and normalize_for_compare(device_name) in PROTECTION_LAYOUT_DEVICES
                    and hasattr(gdf_sup_local, "geometry")
                ):
                    desired_count = len(gdf_sup_local)
                    if seq_entries and len(seq_entries) > desired_count:
                        desired_count = len(seq_entries)
                    anchor = resolve_ups_anchor_point(
                        ups_anchor_info.get("path"),
                        ups_anchor_info.get("layer"),
                        gdf_sup_local.crs,
                    )
                    try:
                        spacing_val = float(ups_anchor_info.get("spacing", PROTECTION_LAYOUT_SPACING))
                    except Exception:
                        spacing_val = PROTECTION_LAYOUT_SPACING
                    if anchor is not None:
                        layout_applied = True
                        if desired_count > len(gdf_sup_local):
                            extra = desired_count - len(gdf_sup_local)
                            extra_points = build_protection_layout_points(anchor, extra, spacing_val)
                            if extra_points and len(extra_points) == extra:
                                extra_rows = pd.DataFrame(
                                    {col: [pd.NA] * extra for col in gdf_sup_local.columns}
                                )
                                gdf_sup_local = pd.concat(
                                    [gdf_sup_local, extra_rows],
                                    ignore_index=True,
                                )
                                if geom_name:
                                    gdf_sup_local = gpd.GeoDataFrame(
                                        gdf_sup_local,
                                        geometry=geom_name,
                                        crs=geom_crs,
                                    )
                                gdf_sup_local = gdf_sup_local.copy()
                                gdf_sup_local.geometry.iloc[-extra:] = extra_points
                fm_local = field_map
                order_local = field_order or []
                if fm_local is None and match_column is None:
                    parsed = parse_supervisor_device_table(sup_wb_path, sup_sheet, device_name)
                    if not parsed:
                        raise ValueError(f"No entries found for device '{device_name}' in sheet '{sup_sheet}'.")
                    # keep parsed instances available for fallback sequential assignment
                    parsed_instances = parsed
                    fm_local = parsed[0].get("fields", {})
                    order_local = parsed[0].get("order", [])
                if fm_local is None and match_column is None:
                    raise ValueError(f"No field values available for device '{device_name}'.")
                out_cols: dict[str, Any] = {}
                if geom_name:
                    out_cols[geom_name] = gdf_sup_local.geometry
                n = len(gdf_sup_local)
                filled_fields: list[str] = []
                if match_column and match_column in gdf_sup_local.columns:
                    out_cols[match_column] = gdf_sup_local[match_column].copy()

                seq_row_indices = list(range(n))
                if hasattr(gdf_sup_local, "geometry"):
                    try:
                        seq_row_indices = order_indices_by_location(gdf_sup_local.geometry)
                    except Exception:
                        seq_row_indices = list(range(n))
                seq_entry_order = _build_seq_entry_order(n, len(seq_entries))
                seq_group_map = None
                if block_assign and seq_entries and hasattr(gdf_sup_local, "geometry"):
                    try:
                        seq_group_map = group_indices_by_perp_gap(gdf_sup_local.geometry, len(seq_entries))
                    except Exception:
                        seq_group_map = None
                prefix_assignment_map: dict[int, dict[str, Any]] | None = None
                if (
                    seq_entries
                    and hasattr(gdf_sup_local, "geometry")
                    and normalize_for_compare(device_name) in PREFIX_GROUP_DEVICES
                ):
                    prefix_groups: dict[str, list[tuple[int | None, dict[str, Any]]]] = {}
                    prefix_order: list[str] = []
                    for inst in seq_entries:
                        ident = inst.get("id") or inst.get("name")
                        res = split_instance_prefix_suffix(ident)
                        if not isinstance(res, tuple) or len(res) != 2:
                            continue
                        prefix, suffix = res
                        if not prefix:
                            continue
                        key = normalize_for_compare(prefix)
                        if key not in prefix_groups:
                            prefix_groups[key] = []
                            prefix_order.append(key)
                        prefix_groups[key].append((suffix, inst))
                    if prefix_groups:
                        for key, items in prefix_groups.items():
                            prefix_groups[key] = sorted(
                                items,
                                key=lambda t: (t[0] is None, t[0] if t[0] is not None else 0),
                            )
                        prefix_group_map = group_indices_by_perp_gap(gdf_sup_local.geometry, len(prefix_groups))
                        group_ids = sorted(set(prefix_group_map.values()))
                        prefix_by_group: dict[int, str] = {}
                        for idx, gid in enumerate(group_ids):
                            prefix_by_group[gid] = prefix_order[idx % len(prefix_order)]
                        prefix_assignment_map = {}
                        for gid in group_ids:
                            pref_key = prefix_by_group.get(gid)
                            if not pref_key:
                                continue
                            entries = [inst for _, inst in prefix_groups.get(pref_key, [])]
                            if not entries:
                                continue
                            row_indices = [idx for idx in seq_row_indices if prefix_group_map.get(idx) == gid]
                            for j, idx_row in enumerate(row_indices):
                                prefix_assignment_map[idx_row] = entries[j % len(entries)]
                spatial_norm_target = None
                if (
                    instance_map
                    and line_bay_info
                    and normalize_for_compare(device_name) in LINE_BAY_SPATIAL_DEVICES
                    and hasattr(gdf_sup_local, "geometry")
                ):
                    try:
                        spatial_norm_target = build_spatial_match_targets(
                            gdf_sup_local,
                            line_bay_info.get("path"),
                            line_bay_info.get("layer"),
                            line_bay_info.get("field"),
                        )
                    except Exception:
                        spatial_norm_target = None

                def _maybe_fill_match_id(idx_row: int, entry: dict[str, Any]) -> None:
                    if not match_column:
                        return
                    if match_column not in out_cols:
                        return
                    try:
                        current_val = out_cols[match_column].iat[idx_row]
                    except Exception:
                        return
                    if pd.isna(current_val) or (isinstance(current_val, str) and current_val.strip() == ""):
                        new_id = entry.get("id") or entry.get("name")
                        if new_id:
                            out_cols[match_column].iat[idx_row] = new_id

                if instance_map and (match_column or spatial_norm_target is not None or layout_applied):
                    match_norm_target = None
                    if match_column:
                        if match_column in gdf_sup_local.columns:
                            match_norm_target = gdf_sup_local[match_column].map(normalize_value_for_compare)
                        elif spatial_norm_target is None:
                            raise ValueError(f"Match column '{match_column}' not found in layer '{layer}'.")
                    if match_norm_target is not None:
                        match_norm_target = match_norm_target.reindex(gdf_sup_local.index)
                    if spatial_norm_target is not None:
                        spatial_norm_target = spatial_norm_target.reindex(gdf_sup_local.index)

                    norm_target = match_norm_target
                    if spatial_norm_target is not None:
                        if norm_target is None:
                            norm_target = spatial_norm_target
                        else:
                            norm_target = spatial_norm_target.copy()
                            missing = norm_target.isna() | (norm_target == "")
                            norm_target.loc[missing] = match_norm_target.loc[missing]
                    if norm_target is None:
                        norm_target = pd.Series([pd.NA] * n, index=gdf_sup_local.index)

                    # initialize output columns for all fields we might fill
                    all_fields_ordered: list[str] = []
                    all_fields_seen: set[str] = set()
                    # honor order from the first instance if available
                    for _, (fields, order) in instance_map.items():
                        for f in order:
                            if f not in all_fields_seen:
                                all_fields_seen.add(f)
                                all_fields_ordered.append(f)
                        for f in fields.keys():
                            if f not in all_fields_seen:
                                all_fields_seen.add(f)
                                all_fields_ordered.append(f)
                    if default_fields:
                        for f in default_fields.keys():
                            if f not in all_fields_seen:
                                all_fields_seen.add(f)
                                all_fields_ordered.append(f)

                    for f in all_fields_ordered:
                        if f == geom_name:
                            continue
                        out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)

                    matched_hits = 0
                    matched_indices: set[int] = set()
                    for idx_val, norm_val in norm_target.items():
                        payload = instance_map.get(norm_val)
                        if payload is None:
                            # If we have multiple instances to distribute, defer filling to the sequential pass.
                            if seq_entries:
                                payload = (None, [])
                            else:
                                payload = (default_fields, [])
                        fields, _order = payload
                        if not fields:
                            continue
                        matched_hits += 1
                        matched_indices.add(idx_val)
                        for f, val in fields.items():
                            if f == geom_name:
                                continue
                            if f not in out_cols:
                                out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                            fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                            out_cols[f].iat[idx_val] = fill_val

                    # If single feature and nothing matched, fill with default or first instance.
                    if matched_hits == 0 and n == 1:
                        fallback_fields = default_fields
                        if fallback_fields is None and instance_map:
                            # take first instance_map entry
                            first_payload = next(iter(instance_map.values()), (None, []))
                            fallback_fields = first_payload[0]
                        if fallback_fields:
                            for f, val in fallback_fields.items():
                                if f == geom_name:
                                    continue
                                if f not in out_cols:
                                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                                fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                                out_cols[f].iat[0] = fill_val
                    # If multi-feature and no matches at all but we have defaults, fill all rows with defaults.
                    if matched_hits == 0 and n > 1 and default_fields:
                        for f, val in default_fields.items():
                            if f == geom_name:
                                continue
                            if f not in out_cols:
                                out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                            fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                            out_cols[f] = pd.Series([fill_val] * n, index=gdf_sup_local.index)
                    # If still no matches and sequential instances are provided, distribute them across rows.
                    if matched_hits == 0 and seq_entries:
                        for row_rank, idx_row in enumerate(seq_row_indices):
                            entry = _pick_seq_entry_by_feeder(
                                idx_row,
                                row_rank,
                                gdf_sup_local,
                                seq_entry_order,
                                seq_group_map,
                                prefix_assignment_map,
                            )
                            inst_fields = entry.get("fields", {})
                            for f, val in inst_fields.items():
                                if f == geom_name:
                                    continue
                                if f not in out_cols:
                                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                                fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                                out_cols[f].iat[idx_row] = fill_val
                            _maybe_fill_match_id(idx_row, entry)

                    # If some rows remain unmatched, fill those rows using sequential instances (feeder-aware) without overwriting matched rows.
                    if (seq_entries and len(matched_indices) < n) or (
                        not seq_entries
                        and 'parsed_instances' in locals()
                        and len(parsed_instances) > 1
                        and len(matched_indices) < n
                        and seq_assign_fallback
                    ):
                        # ensure we have seq_entries list to consume
                        if not seq_entries and 'parsed_instances' in locals() and len(parsed_instances) > 1:
                            # build seq_entries from parsed_instances (fields + optional id/name)
                            for inst in parsed_instances:
                                if isinstance(inst, dict) and "fields" in inst:
                                    seq_entries.append({
                                        "fields": inst.get("fields", {}) or {},
                                        "id": inst.get("id_value"),
                                        "name": inst.get("name_value"),
                                    })
                                else:
                                    seq_entries.append({"fields": inst if isinstance(inst, dict) else {}, "id": None, "name": None})
                            seq_entry_order = _build_seq_entry_order(n, len(seq_entries))
                            if block_assign and hasattr(gdf_sup_local, "geometry"):
                                try:
                                    seq_group_map = group_indices_by_perp_gap(gdf_sup_local.geometry, len(seq_entries))
                                except Exception:
                                    seq_group_map = None
                            if (
                                seq_entries
                                and hasattr(gdf_sup_local, "geometry")
                                and normalize_for_compare(device_name) in PREFIX_GROUP_DEVICES
                            ):
                                prefix_groups = {}
                                prefix_order = []
                                for inst in seq_entries:
                                    ident = inst.get("id") or inst.get("name")
                                    res = split_instance_prefix_suffix(ident)
                                    if not isinstance(res, tuple) or len(res) != 2:
                                        continue
                                    prefix, suffix = res
                                    if not prefix:
                                        continue
                                    key = normalize_for_compare(prefix)
                                    if key not in prefix_groups:
                                        prefix_groups[key] = []
                                        prefix_order.append(key)
                                    prefix_groups[key].append((suffix, inst))
                                if prefix_groups:
                                    for key, items in prefix_groups.items():
                                        prefix_groups[key] = sorted(
                                            items,
                                            key=lambda t: (t[0] is None, t[0] if t[0] is not None else 0),
                                        )
                                    prefix_group_map = group_indices_by_perp_gap(
                                        gdf_sup_local.geometry, len(prefix_groups)
                                    )
                                    group_ids = sorted(set(prefix_group_map.values()))
                                    prefix_by_group = {}
                                    for idx, gid in enumerate(group_ids):
                                        prefix_by_group[gid] = prefix_order[idx % len(prefix_order)]
                                    prefix_assignment_map = {}
                                    for gid in group_ids:
                                        pref_key = prefix_by_group.get(gid)
                                        if not pref_key:
                                            continue
                                        entries = [inst for _, inst in prefix_groups.get(pref_key, [])]
                                        if not entries:
                                            continue
                                        row_indices = [
                                            idx
                                            for idx in seq_row_indices
                                            if prefix_group_map.get(idx) == gid
                                        ]
                                        for j, idx_row in enumerate(row_indices):
                                            prefix_assignment_map[idx_row] = entries[j % len(entries)]

                        for row_rank, idx_row in enumerate(seq_row_indices):
                            if idx_row in matched_indices:
                                continue
                            entry = _pick_seq_entry_by_feeder(
                                idx_row,
                                row_rank,
                                gdf_sup_local,
                                seq_entry_order,
                                seq_group_map,
                                prefix_assignment_map,
                            )
                            inst_fields = entry.get("fields", {})
                            for f, val in inst_fields.items():
                                if f == geom_name:
                                    continue
                                if f not in out_cols:
                                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                                if pd.isna(out_cols[f].iat[idx_row]):
                                    fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                                    out_cols[f].iat[idx_row] = fill_val
                            _maybe_fill_match_id(idx_row, entry)

                    filled_fields = [f for f in out_cols.keys() if f != geom_name]
                else:
                    if seq_entries:
                        for row_rank, idx_row in enumerate(seq_row_indices):
                            entry = _pick_seq_entry_by_feeder(
                                idx_row,
                                row_rank,
                                gdf_sup_local,
                                seq_entry_order,
                                seq_group_map,
                                prefix_assignment_map,
                            )
                            inst_fields = entry.get("fields", {})
                            for f, val in inst_fields.items():
                                if f == geom_name:
                                    continue
                                if f not in out_cols:
                                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                                fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                                out_cols[f].iat[idx_row] = fill_val
                            _maybe_fill_match_id(idx_row, entry)
                        filled_fields = [f for f in out_cols.keys() if f != geom_name]
                    else:
                        ordered_keys = order_local if order_local else list(fm_local.keys())
                        for f in ordered_keys:
                            val = fm_local.get(f)
                            if val is None:
                                continue
                            target_col = f
                            if target_col not in out_cols:
                                out_cols[target_col] = pd.NA
                            if isinstance(val, pd.Series):
                                fill_val = val.iloc[0] if not val.empty else pd.NA
                            else:
                                fill_val = val
                            out_cols[target_col] = pd.Series([fill_val] * n, index=gdf_sup_local.index)
                            filled_fields.append(target_col)

                keep_cols = filled_fields.copy()
                if match_column and match_column in out_cols:
                    norm_keep = {normalize_for_compare(c) for c in keep_cols}
                    if normalize_for_compare(match_column) not in norm_keep:
                        keep_cols.append(match_column)
                if geom_name and geom_name not in keep_cols:
                    keep_cols.append(geom_name)

                # Drop utility columns (e.g., Composite_ID) from the output.
                keep_cols = [c for c in keep_cols if normalize_for_compare(c) not in DROP_OUTPUT_COLUMNS]

                # preserve column order where possible
                out_gdf = gpd.GeoDataFrame(
                    {c: out_cols[c] for c in keep_cols if c in out_cols},
                    geometry=gdf_sup_local.geometry if hasattr(gdf_sup_local, "geometry") else None,
                    crs=gdf_sup_local.crs,
                )
                out_gdf = sanitize_gdf_for_gpkg(out_gdf)
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                    out_path = Path(tmpout.name)
                out_gdf.to_file(out_path, driver="GPKG", layer=layer)
                return out_path, layer

            if len(sup_gpkg_files) == 1:
                sup_gpkg = sup_gpkg_files[0]
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                    tmp.write(sup_gpkg.getbuffer())
                    sup_gpkg_path = Path(tmp.name)
                sup_layers = list_gpkg_layers(sup_gpkg_path)
                sup_layer = st.selectbox("Select layer", sup_layers if sup_layers else [])
                match_column_choice = None
                if sup_layers and fill_mode == "Match rows to instances (single GPKG)":
                    try:
                        gdf_preview = gpd.read_file(sup_gpkg_path, layer=sup_layer)
                        candidate_cols = [c for c in gdf_preview.columns if c != gdf_preview.geometry.name] if hasattr(gdf_preview, "geometry") else list(gdf_preview.columns)
                        pref_cols = preferred_match_columns(device_choice)
                        file_pref_cols = match_overrides_for_file(sup_gpkg.name)
                        pref_cols = file_pref_cols + [c for c in pref_cols if c not in file_pref_cols]

                        def _score_col(col: str) -> int:
                            norm = normalize_for_compare(col)
                            score = 0
                            for kw in ["id", "name", "bay", "switch", "gear", "line", "feeder", "arrester", "lightning", "substation"]:
                                if kw in norm:
                                    score += 1
                            return score

                        default_col = None
                        if candidate_cols:
                            lookup = {normalize_for_compare(c): c for c in candidate_cols}
                            for pref in pref_cols:
                                n = normalize_for_compare(pref)
                                if n in lookup:
                                    default_col = lookup[n]
                                    break
                            if default_col is None and len(gdf_preview) <= 1:
                                # single-feature fallback to substation columns if present
                                for pref in ["Substation ID", "SubstationID", "SUBSTATION NAMES"]:
                                    n = normalize_for_compare(pref)
                                    if n in lookup:
                                        default_col = lookup[n]
                                        break
                            if default_col is None:
                                scored = sorted(candidate_cols, key=lambda c: (-_score_col(c), len(c)))
                                default_col = scored[0]
                            match_column_choice = st.selectbox("Match supervisor instances to this column", candidate_cols, index=candidate_cols.index(default_col))
                    except Exception:
                        st.warning("Could not auto-inspect the GeoPackage to suggest a match column.")
                if sup_layers and st.button("Fill attributes from supervisor sheet", key="sup_fill"):
                    try:
                        if fill_mode == "One GeoPackage per instance" and instance_labels:
                            outputs: list[tuple[str, Path]] = []
                            for inst in device_instances:
                                out_path, layer_name = fill_one_gpkg(
                                    sup_gpkg,
                                    device_choice,
                                    sup_layer,
                                    field_map=inst.get("fields"),
                                    field_order=inst.get("order"),
                                    line_bay_info=line_bay_info,
                                    ups_anchor_info=ups_anchor_info,
                                )
                                # create a friendly name per instance
                                label_slug = normalize_for_compare(inst.get("label", "instance")).replace(" ", "_")[:40]
                                fname = f"{Path(sup_gpkg.name).stem}_{label_slug}.gpkg"
                                outputs.append((fname, out_path))

                            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as ztmp:
                                zip_path = Path(ztmp.name)
                            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                for fname, out_path in outputs:
                                    zf.write(out_path, arcname=fname)
                            with open(zip_path, "rb") as f:
                                data = f.read()
                            st.download_button(
                                "Download per-instance GeoPackages (zip)",
                                data=data,
                                file_name=f"{Path(sup_gpkg.name).stem}_instances.zip",
                                mime="application/zip",
                                key="sup_download_instances",
                            )
                        elif fill_mode == "Match rows to instances (single GPKG)" and instance_labels:
                            use_line_bay_match = line_bay_info is not None
                            use_ups_layout = (
                                ups_anchor_info is not None
                                and normalize_for_compare(device_choice) in PROTECTION_LAYOUT_DEVICES
                            )
                            if not match_column_choice and not use_line_bay_match and not use_ups_layout:
                                raise ValueError("Please select a column to match supervisor instances against.")
                            # build instance map
                            inst_map: dict[str, tuple[dict[str, Any], list[str]]] = {}
                            for inst in device_instances:
                                fields = inst.get("fields", {})
                                order = inst.get("order", [])
                                id_val = inst.get("id_value")
                                feeder_val = inst.get("feeder_value")
                                name_val = inst.get("name_value")
                                candidates = [id_val, name_val, feeder_val]
                                # combined key: id + feeder
                                if pd.notna(id_val) and pd.notna(feeder_val):
                                    candidates.append(f"{id_val}_{feeder_val}")
                                    candidates.append(f"{feeder_val}_{id_val}")
                                # feeder-type heuristics for indoor MV devices (MV1 -> transformer feeder, MV3 -> line feeder)
                                try:
                                    id_norm = normalize_for_compare(id_val)
                                except Exception:
                                    id_norm = ""
                                if "feeder" in normalize_for_compare(match_column_choice or ""):
                                    if "mv1" in id_norm or id_norm.endswith("1"):
                                        candidates.append("transformer feeder")
                                        candidates.append("transformer_feeder")
                                    if "mv3" in id_norm or id_norm.endswith("3"):
                                        candidates.append("line feeder")
                                        candidates.append("line_feeder")
                                for cand in candidates:
                                    norm = normalize_value_for_compare(cand)
                                    if norm and norm not in inst_map:
                                        inst_map[norm] = (fields, order)
                            seq_arg = None
                            if len(device_instances) > 1:
                                seq_arg = device_instances
                            elif normalize_for_compare(device_choice) in SEQUENTIAL_FILL_DEVICES:
                                seq_arg = device_instances
                            out_path, layer_name = fill_one_gpkg(
                                sup_gpkg,
                                device_choice,
                                sup_layer,
                                match_column=match_column_choice,
                                instance_map=inst_map,
                                default_fields=selected_instance.get("fields") if selected_instance else None,
                                field_order=selected_instance.get("order") if selected_instance else None,
                                sequential_instances=seq_arg,
                                line_bay_info=line_bay_info,
                                ups_anchor_info=ups_anchor_info,
                            )
                            with open(out_path, "rb") as f:
                                data_bytes = f.read()
                            st.download_button(
                                "Download filled GeoPackage",
                                data=data_bytes,
                                file_name=sup_gpkg.name,
                                mime="application/geopackage+sqlite3",
                                key="sup_download_rowmatch",
                            )
                        else:
                            out_path, layer_name = fill_one_gpkg(
                                sup_gpkg,
                                device_choice,
                                sup_layer,
                                field_map=selected_instance.get("fields") if selected_instance else None,
                                field_order=selected_instance.get("order") if selected_instance else None,
                                line_bay_info=line_bay_info,
                                ups_anchor_info=ups_anchor_info,
                            )
                            with open(out_path, "rb") as f:
                                data_bytes = f.read()
                            st.download_button(
                                "Download filled GeoPackage",
                                data=data_bytes,
                                file_name=sup_gpkg.name,
                                mime="application/geopackage+sqlite3",
                                key="sup_download",
                            )
                    except Exception as exc:
                        st.error(f"Supervisor fill failed: {exc}")
            else:
                st.info(f"{len(sup_gpkg_files)} GeoPackages uploaded; the first layer of each will be filled automatically using a per-file device match.")
                if st.button("Fill all uploaded GeoPackages", key="sup_fill_all"):
                    logs: list[str] = []
                    outputs: list[tuple[str, Path]] = []
                    instance_cache: dict[str, list[dict[str, Any]]] = {}
                    uploaded_device_norms: set[str] = set()

                    def _write_original_file(file_obj):
                        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                            tmp.write(file_obj.getbuffer())
                            return Path(tmp.name)

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

                    for file_obj in sup_gpkg_files:
                        try:
                            stem_norm = normalize_for_compare(Path(file_obj.name).stem)
                            if stem_norm in SKIP_BATCH_FILL_STEMS:
                                out_path = _write_original_file(file_obj)
                                outputs.append((file_obj.name, out_path))
                                logs.append(f"{file_obj.name}: skipped fill (kept original geometry).")
                                continue
                            device_for_file = resolve_equipment_name(file_obj.name, device_options, equip_map_sup)
                            uploaded_device_norms.add(normalize_for_compare(device_for_file))
                            if device_for_file not in instance_cache:
                                instance_cache[device_for_file] = parse_supervisor_device_table(
                                    sup_wb_path, sup_sheet, device_for_file
                                )
                            inst = _pick_instance_for_file(file_obj.name, instance_cache.get(device_for_file, []))
                            seq_arg = None
                            cached_instances = instance_cache.get(device_for_file, [])
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
                            out_path, used_layer = fill_one_gpkg(
                                file_obj,
                                device_for_file,
                                field_map=inst.get("fields") if inst else None,
                                field_order=inst.get("order") if inst else None,
                                instance_map=inst_map,
                                default_fields=default_fields,
                                sequential_instances=seq_arg,
                                line_bay_info=line_bay_info,
                                ups_anchor_info=ups_anchor_info,
                            )
                            outputs.append((file_obj.name, out_path))
                            chosen_label = inst.get("label") if inst else "default instance"
                            logs.append(
                                f"{file_obj.name}: filled using device '{device_for_file}' ({chosen_label}) on layer '{used_layer}'."
                            )
                        except Exception as exc:
                            out_path = _write_original_file(file_obj)
                            outputs.append((file_obj.name, out_path))
                            logs.append(f"{file_obj.name}: failed ({exc}); kept original file.")

                    if ups_anchor_info:
                        protection_devices = [
                            dev
                            for dev in device_options
                            if normalize_for_compare(dev) in PROTECTION_LAYOUT_DEVICES
                        ]
                        anchor, anchor_crs = load_ups_anchor_and_crs(
                            ups_anchor_info.get("path"),
                            ups_anchor_info.get("layer"),
                        )
                        try:
                            spacing_val = float(ups_anchor_info.get("spacing", PROTECTION_LAYOUT_SPACING))
                        except Exception:
                            spacing_val = PROTECTION_LAYOUT_SPACING
                        if anchor is None:
                            logs.append("Protection auto-create skipped: UPS anchor could not be resolved.")
                        else:
                            for dev_name in protection_devices:
                                if normalize_for_compare(dev_name) in uploaded_device_norms:
                                    continue
                                instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
                                if not instances:
                                    continue
                                points = build_protection_layout_points(anchor, len(instances), spacing_val)
                                if not points or len(points) != len(instances):
                                    logs.append(f"{dev_name}: protection layout failed (no points).")
                                    continue
                                out_gdf = build_device_gdf_from_instances(instances, points, anchor_crs)
                                layer_name = derive_layer_name_from_filename(f"{dev_name}.gpkg")
                                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                                    out_path = Path(tmpout.name)
                                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                                file_name = f"{layer_name}.gpkg"
                                outputs.append((file_name, out_path))
                                logs.append(f"{dev_name}: auto-created protection points ({len(points)}).")

                    template_devices = [
                        ("High Voltage Line", HV_LINE_TEMPLATE_PATH),
                        ("Earthing Transformer", EARTHING_TRANSFORMER_TEMPLATE_PATH),
                    ]
                    for dev_name, tpl_path in template_devices:
                        dev_norm = normalize_for_compare(dev_name)
                        if dev_norm in uploaded_device_norms:
                            continue
                        instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, dev_name)
                        if not instances:
                            logs.append(f"{dev_name}: skipped (no instances in sheet).")
                            continue
                        if dev_norm == normalize_for_compare("High Voltage Line") and line_bay_info:
                            bay_gdf = load_line_bay_layer(
                                line_bay_info.get("path"),
                                line_bay_info.get("layer"),
                                line_bay_info.get("field"),
                            )
                            if bay_gdf is not None and not bay_gdf.empty:
                                bay_field = line_bay_info.get("field")
                                geom_col = bay_gdf.geometry.name
                                geoms_all = list(bay_gdf[geom_col])
                                by_norm: dict[str, list[int]] = {}
                                for idx, row in bay_gdf.iterrows():
                                    name_val = row.get(bay_field)
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
                                preferred_points = collect_device_points_from_uploads(
                                    sup_gpkg_files, bay_gdf.crs, device_options, equip_map_sup, lightning_norms
                                )
                                all_points = collect_point_geometries_from_uploads(sup_gpkg_files, bay_gdf.crs)
                                points_source = preferred_points if preferred_points is not None and not preferred_points.empty else all_points
                                points_by_bay = map_points_to_bays(points_source, bay_gdf) if points_source is not None else {}

                                expanded_instances: list[dict[str, Any]] = []
                                expanded_geoms: list[Any] = []
                                for inst in instances:
                                    candidates = [inst.get("id_value"), inst.get("name_value"), inst.get("feeder_value")]
                                    chosen_idx = None
                                    for cand in candidates:
                                        norm = normalize_value_for_compare(cand)
                                        if norm and norm in by_norm and by_norm[norm]:
                                            chosen_idx = by_norm[norm].pop(0)
                                            if chosen_idx in unused_set:
                                                unused_set.remove(chosen_idx)
                                            break
                                    if chosen_idx is None:
                                        chosen_idx = _take_unused()
                                    if chosen_idx is None and geoms_all:
                                        chosen_idx = 0
                                    if chosen_idx is None:
                                        continue
                                    poly = geoms_all[chosen_idx]
                                    points_in_bay = points_by_bay.get(chosen_idx, [])
                                    lines = build_lines_from_points_in_polygon(poly, points_in_bay, 3)
                                    lines = expand_geometries(lines, 3)
                                    if not lines:
                                        continue
                                    for ln in lines:
                                        expanded_instances.append(inst)
                                        expanded_geoms.append(ln)
                                if expanded_geoms:
                                    out_gdf = build_device_gdf_from_instances(
                                        expanded_instances, expanded_geoms, bay_gdf.crs
                                    )
                                    layer_name = derive_layer_name_from_filename(dev_name)
                                    file_name = f"{dev_name}.gpkg"
                                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                                        out_path = Path(tmpout.name)
                                    out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                                    outputs.append((file_name, out_path))
                                    logs.append(
                                        f"{dev_name}: auto-created from Line Bay polygons ({len(expanded_geoms)} feature(s))."
                                    )
                                    continue
                        if dev_norm == normalize_for_compare("Earthing Transformer"):
                            cabin_norms = {normalize_for_compare("Substation/Cabin")}
                            cabins_gdf = collect_device_polygons_from_uploads(
                                sup_gpkg_files, None, device_options, equip_map_sup, cabin_norms
                            )
                            switchgear_norms = {
                                normalize_for_compare("MV Switch gear"),
                                normalize_for_compare("INDOR SWITCHGEAR TABLE"),
                            }
                            switchgear_pts = collect_device_points_from_uploads(
                                sup_gpkg_files, cabins_gdf.crs if cabins_gdf is not None else None, device_options, equip_map_sup, switchgear_norms
                            )
                            geoms: list[Any] = []
                            if cabins_gdf is not None and not cabins_gdf.empty:
                                try:
                                    if switchgear_pts is not None and not switchgear_pts.empty and switchgear_pts.crs != cabins_gdf.crs:
                                        switchgear_pts = switchgear_pts.to_crs(cabins_gdf.crs)
                                except Exception:
                                    pass
                                for _, cabin in cabins_gdf.iterrows():
                                    poly = cabin.geometry
                                    anchor = None
                                    if switchgear_pts is not None and not switchgear_pts.empty:
                                        try:
                                            pts_inside = switchgear_pts[switchgear_pts.within(poly)]
                                        except Exception:
                                            pts_inside = gpd.GeoDataFrame()
                                        if not pts_inside.empty:
                                            try:
                                                anchor = pts_inside.unary_union.centroid
                                            except Exception:
                                                anchor = pts_inside.iloc[0].geometry
                                    if anchor is None:
                                        try:
                                            anchor = poly.centroid
                                        except Exception:
                                            anchor = None
                                    if anchor is not None:
                                        geoms.append(anchor)
                            if geoms:
                                target_count = len(instances)
                                geoms = expand_geometries(geoms, target_count)
                                out_gdf = build_device_gdf_from_instances(instances, geoms, cabins_gdf.crs if cabins_gdf is not None else None)
                                layer_name = derive_layer_name_from_filename(dev_name)
                                file_name = f"{dev_name}.gpkg"
                                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                                    out_path = Path(tmpout.name)
                                out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                                outputs.append((file_name, out_path))
                                logs.append(f"{dev_name}: auto-created beside switchgear inside cabins ({len(geoms)} feature(s)).")
                                continue
                        tpl = load_template_layer(tpl_path)
                        if tpl is None:
                            logs.append(f"{dev_name}: template not found at {tpl_path}.")
                            continue
                        tpl_gdf, _tpl_layer = tpl
                        geoms = list(tpl_gdf.geometry)
                        if dev_norm == normalize_for_compare("High Voltage Line"):
                            instances = repeat_instances(instances, 3)
                        target_count = len(instances)
                        if target_count <= 0:
                            logs.append(f"{dev_name}: skipped (no instances to fill).")
                            continue
                        geoms = expand_geometries(geoms, target_count)
                        if not geoms:
                            logs.append(f"{dev_name}: template has no geometry.")
                            continue
                        out_gdf = build_device_gdf_from_instances(instances, geoms, tpl_gdf.crs)
                        layer_name = derive_layer_name_from_filename(dev_name)
                        file_name = f"{dev_name}.gpkg"
                        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
                            out_path = Path(tmpout.name)
                        out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                        outputs.append((file_name, out_path))
                        logs.append(f"{dev_name}: auto-created from template ({len(geoms)} feature(s)).")

                    if outputs:
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as ztmp:
                            zip_path = Path(ztmp.name)
                        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            for name, out_path in outputs:
                                zf.write(out_path, arcname=Path(name).name)
                        with open(zip_path, "rb") as f:
                            data = f.read()
                        st.download_button(
                            "Download filled GeoPackages (zip)",
                            data=data,
                            file_name="filled_supervisor_gpkgs.zip",
                            mime="application/zip",
                            key="sup_download_zip",
                        )
                    st.text_area("Supervisor fill log", value="\n".join(logs) if logs else "No logs.", height=180)
        finally:
            pass

    if map_file is not None:
        temp_map_path = None
        temp_gdb_dir = None
        try:
            if source_type.startswith("GeoPackage"):
                with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
                    tmp.write(map_file.getbuffer())
                    temp_map_path = Path(tmp.name)
            else:
                ext = Path(map_file.name).suffix.lower()
                if ext == ".zip":
                    temp_gdb_dir = Path(tempfile.mkdtemp())
                    zip_path = temp_gdb_dir / "gdb.zip"
                    with open(zip_path, "wb") as tmp:
                        tmp.write(map_file.getbuffer())
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        zf.extractall(temp_gdb_dir)
                    gdb_dirs = list(temp_gdb_dir.glob("**/*.gdb"))
                    if not gdb_dirs:
                        st.error("No .gdb folder found inside the zip.")
                        return
                    temp_map_path = gdb_dirs[0]
                elif ext == ".gdb":
                    # Browsers typically cannot upload a .gdb folder directly; advise zipping
                    st.error("Please upload the FileGDB as a .zip containing the .gdb folder.")
                    return
                else:
                    st.error("Unsupported FileGDB upload. Please zip the .gdb folder.")
                    return

            layers_map = list_gpkg_layers(temp_map_path)
            layer_map = st.selectbox("Select layer", layers_map if layers_map else [])
            if not layers_map:
                st.error("No layers found in the uploaded GeoPackage.")
            else:
                gdf_map = gpd.read_file(temp_map_path, layer=layer_map)
                st.write(f"Loaded **{len(gdf_map):,}** feature(s) from layer **{layer_map}**.")

                # Schema selection
                schema_files = list_reference_workbooks()
                if not schema_files:
                    st.error("No reference workbooks found in reference_data.")
                else:
                    schema_label = st.selectbox("Schema workbook", list(schema_files.keys()), index=0, key="schema_wb")
                    schema_path = schema_files[schema_label]
                    schema_excel = pd.ExcelFile(schema_path)
                    schema_sheet = st.selectbox("Schema sheet", schema_excel.sheet_names, key="schema_sheet")

                    # Choose equipment/device from schema
                    equipment_options = list_schema_equipments(schema_path, schema_sheet)
                    if not equipment_options:
                        st.error("No equipment entries found in the schema sheet.")
                    else:
                        equip_map = load_gpkg_equipment_map()
                        norm_gpkg = normalize_for_compare(Path(map_file.name).stem)
                        mapped_equipment = equip_map.get(norm_gpkg)
                        # fallback heuristic: choose best similarity if no explicit mapping
                        default_equip_idx = 0
                        if mapped_equipment and mapped_equipment in equipment_options:
                            default_equip_idx = equipment_options.index(mapped_equipment)
                        else:
                            try:
                                import difflib

                                best = difflib.get_close_matches(
                                    norm_gpkg, [normalize_for_compare(e) for e in equipment_options], n=1, cutoff=0.5
                                )
                                if best:
                                    match_norm = best[0]
                                    for i, opt in enumerate(equipment_options):
                                        if normalize_for_compare(opt) == match_norm:
                                            default_equip_idx = i
                                            break
                            except Exception:
                                pass

                        equipment_name = st.selectbox(
                            "Equipment/device", equipment_options, index=default_equip_idx, key="schema_equipment"
                        )

                        # Load fields/types for selected equipment
                        schema_fields, type_map = load_schema_fields(schema_path, schema_sheet, equipment_name)

                        # Show schema preview
                        preview_rows = [{"Field": f, "Type": type_map.get(f, "")} for f in schema_fields]
                        st.subheader("Selected Equipment Schema")
                        st_dataframe_safe(pd.DataFrame(preview_rows))

                        # Suggested mapping with adjustable sensitivity
                        mapping_threshold = st.slider(
                            "Auto-mapping sensitivity (lower = more aggressive suggestions)",
                            min_value=0.0,
                            max_value=1.0,
                            value=0.35,
                            step=0.05,
                            key="map_threshold",
                        )
                        exclude_cols = {gdf_map.geometry.name} if hasattr(gdf_map, "geometry") else set()
                        suggested, score_map = fuzzy_map_columns_with_scores(
                            list(gdf_map.columns), schema_fields, threshold=mapping_threshold, exclude=exclude_cols
                        )
                        accept_threshold = 0.6
                        norm_col_lookup = {normalize_for_compare(c): c for c in gdf_map.columns}

                        # Confidence hints
                        st.subheader("Field Mapping")
                        st.caption(
                            "Suggested source columns are preselected; adjust as needed. Score shown when a suggestion exists."
                        )

                        mapping = {}
                        cache = load_mapping_cache()
                        cache_key = f"{schema_label}::{schema_sheet}::{equipment_name}"
                        cached_map = cache.get(cache_key, {})
                        for idx, field in enumerate(schema_fields):
                            best_src = suggested.get(field)
                            score = score_map.get(field, 0.0)
                            resolved_src = None
                            # cached choice takes precedence if still present
                            cached_src = cached_map.get(field)
                            if cached_src and cached_src in gdf_map.columns:
                                resolved_src = cached_src
                            if best_src and score >= accept_threshold:
                                resolved_src = norm_col_lookup.get(normalize_for_compare(best_src), best_src)
                                if resolved_src not in gdf_map.columns:
                                    resolved_src = None
                            label = f"{field}"
                            if best_src:
                                label = f"{field} (suggested: {best_src}, score={score:.2f}{' auto-applied' if resolved_src else ''})"
                            options = ["(empty)"] + list(gdf_map.columns)
                            default_index = (options.index(resolved_src) if resolved_src in options else 0)
                            state_key = f"map_select::{schema_label}::{schema_sheet}::{equipment_name}::{idx}"
                            # Ensure session state honors the latest suggestion; reset if option set disappears.
                            if state_key not in st.session_state or st.session_state[state_key] not in options:
                                st.session_state[state_key] = options[default_index]
                            # If a new suggestion arrives, refresh the default.
                            elif resolved_src and st.session_state[state_key] == "(empty)" and default_index != 0:
                                st.session_state[state_key] = options[default_index]
                            mapping[field] = st.selectbox(
                                label,
                                options=options,
                                key=state_key,
                            )

                        keep_unmatched = st.checkbox("Keep unmatched original columns (prefixed with orig_)", value=True)

                        output_formats = ["GeoPackage (gpkg)"]
                        if source_type.startswith("FileGDB"):
                            output_formats.append("FileGDB (zip)")
                        output_choice = st.selectbox(
                            "Output format",
                            output_formats,
                            index=1 if source_type.startswith("FileGDB") and len(output_formats) > 1 else 0,
                            key="map_output_format",
                        )

                        if st.button("Generate Standardized GPKG", key="gen_std_gpkg"):
                            try:
                                out_cols = {}
                                for f in schema_fields:
                                    src = mapping.get(f)
                                    if src and src != "(empty)" and src in gdf_map.columns:
                                        out_cols[f] = gdf_map[src]
                                    else:
                                        out_cols[f] = pd.NA
                                if keep_unmatched:
                                    for col in gdf_map.columns:
                                        if col not in mapping.values() and col != gdf_map.geometry.name:
                                            out_cols[f"orig_{col}"] = gdf_map[col]

                                geom_col = gdf_map.geometry.name if hasattr(gdf_map, "geometry") else None
                                geom_series = None
                                if geom_col and geom_col in gdf_map.columns:
                                    geom_series = gdf_map[geom_col]
                                elif hasattr(gdf_map, "geometry"):
                                    geom_series = gdf_map.geometry

                                # Apply schema types
                                for f in schema_fields:
                                    out_cols[f] = coerce_series_to_type(out_cols[f], type_map.get(f, ""))

                                out_gdf = gpd.GeoDataFrame(out_cols, geometry=geom_series, crs=gdf_map.crs)
                                out_gdf = sanitize_gdf_for_gpkg(out_gdf)

                                # persist user mapping choices
                                chosen_map = {
                                    f: mapping.get(f)
                                    for f in schema_fields
                                    if mapping.get(f) and mapping.get(f) != "(empty)"
                                }
                                cache[cache_key] = chosen_map
                                save_mapping_cache(cache)

                                layer_name = derive_layer_name_from_filename(map_file.name)
                                if output_choice.startswith("GeoPackage"):
                                    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp_out:
                                        out_path = tmp_out.name
                                    out_gdf.to_file(out_path, driver="GPKG", layer=layer_name)
                                    with open(out_path, "rb") as f:
                                        data_bytes = f.read()
                                    st.download_button(
                                        "Download Standardized GeoPackage",
                                        data=data_bytes,
                                        file_name=map_file.name,
                                        mime="application/geopackage+sqlite3",
                                    )
                                else:
                                    tmp_dir = tempfile.mkdtemp()
                                    out_dir = Path(tmp_dir) / f"{layer_name}.gdb"
                                    out_gdf.to_file(out_dir, driver="FileGDB", layer=layer_name)
                                    zip_path = shutil.make_archive(str(out_dir), "zip", root_dir=tmp_dir, base_dir=out_dir.name)
                                    with open(zip_path, "rb") as f:
                                        data_bytes = f.read()
                                    st.download_button(
                                        "Download Standardized FileGDB (zip)",
                                        data=data_bytes,
                                        file_name=f"{out_dir.name}.zip",
                                        mime="application/zip",
                                    )
                                    shutil.rmtree(tmp_dir, ignore_errors=True)
                            except Exception as exc:
                                st.error(f"Schema mapping failed: {exc}")

                        # ---------------- BATCH MODE ----------------
                        st.markdown("---")
                        st.subheader("Batch Map Multiple Layers")
                        selected_layers = st.multiselect("Select layers to batch map", layers_map, default=layers_map)
                        if st.button("Generate Batch Standardized Package", key="gen_batch"):
                            try:
                                default_driver = "FileGDB" if source_type.startswith("FileGDB") else "GPKG"
                                tmp_dir = Path(tempfile.mkdtemp())
                                out_path = tmp_dir / ("mapped.gdb" if default_driver == "FileGDB" else "mapped.gpkg")
                                driver = default_driver

                                for lyr in selected_layers:
                                    gdf_layer = gpd.read_file(temp_map_path, layer=lyr)
                                    exclude_layer_cols = {gdf_layer.geometry.name} if hasattr(gdf_layer, "geometry") else set()
                                    suggested_batch, score_map_batch = fuzzy_map_columns_with_scores(
                                        list(gdf_layer.columns), schema_fields, threshold=mapping_threshold, exclude=exclude_layer_cols
                                    )
                                    norm_col_lookup_batch = {normalize_for_compare(c): c for c in gdf_layer.columns}
                                    out_cols_batch = {}
                                    n = len(gdf_layer)
                                    def _na_series():
                                        return pd.Series([pd.NA] * n, index=gdf_layer.index)
                                    for f in schema_fields:
                                        src = suggested_batch.get(f)
                                        score = score_map_batch.get(f, 0.0)
                                        chosen_src = None
                                        if src and score >= 0.6:
                                            resolved = norm_col_lookup_batch.get(normalize_for_compare(src), src)
                                            if resolved in gdf_layer.columns:
                                                chosen_src = resolved
                                        out_cols_batch[f] = gdf_layer[chosen_src] if chosen_src else _na_series()
                                    if keep_unmatched:
                                        for col in gdf_layer.columns:
                                            if col not in suggested_batch.values() and col != gdf_layer.geometry.name:
                                                out_cols_batch[f"orig_{col}"] = gdf_layer[col]
                                    geom_series = gdf_layer.geometry if hasattr(gdf_layer, "geometry") else None
                                    for f in schema_fields:
                                        out_cols_batch[f] = coerce_series_to_type(out_cols_batch[f], type_map.get(f, ""))
                                    out_layer = gpd.GeoDataFrame(out_cols_batch, geometry=geom_series, crs=gdf_layer.crs)
                                    out_layer = sanitize_gdf_for_gpkg(out_layer)
                                    layer_name_out = derive_layer_name_from_filename(lyr)
                                    try:
                                        out_layer.to_file(out_path, driver=driver, layer=layer_name_out)
                                    except Exception:
                                        # fallback to GPKG if FileGDB driver unavailable
                                        driver = "GPKG"
                                        # clean any previous gdb remnants
                                        if out_path.exists():
                                            if out_path.is_dir():
                                                shutil.rmtree(out_path, ignore_errors=True)
                                            else:
                                                out_path.unlink(missing_ok=True)
                                        out_path = tmp_dir / "mapped.gpkg"
                                        out_layer.to_file(out_path, driver=driver, layer=layer_name_out)

                                if driver == "GPKG":
                                    with open(out_path, "rb") as f:
                                        data_bytes = f.read()
                                    st.download_button(
                                        "Download Batch Standardized GeoPackage",
                                        data=data_bytes,
                                        file_name="standardized_layers.gpkg",
                                        mime="application/geopackage+sqlite3",
                                        key="dl_batch_gpkg",
                                    )
                                    out_path.unlink(missing_ok=True)
                                else:
                                    zip_path = shutil.make_archive(str(out_path), "zip", root_dir=out_path.parent, base_dir=out_path.name)
                                    with open(zip_path, "rb") as f:
                                        data_bytes = f.read()
                                    st.download_button(
                                        "Download Batch Standardized FileGDB (zip)",
                                        data=data_bytes,
                                        file_name="standardized_layers.gdb.zip",
                                        mime="application/zip",
                                        key="dl_batch_gdb",
                                    )
                                    shutil.rmtree(tmp_dir, ignore_errors=True)
                            except Exception as exc:
                                st.error(f"Batch mapping failed: {exc}")
        finally:
            if temp_gdb_dir:
                shutil.rmtree(temp_gdb_dir, ignore_errors=True)
            elif temp_map_path and temp_map_path.exists():
                # Only unlink files, not folders
                try:
                    temp_map_path.unlink()
                except IsADirectoryError:
                    shutil.rmtree(temp_map_path, ignore_errors=True)

    # =====================================================================
    # AUTOMATED SCHEMA MAPPING (ZIP)
    # =====================================================================
    st.markdown("---")
    st.header("Automated Schema Mapping (ZIP)")
    st.caption(
        "Upload a ZIP containing GeoPackages (or zipped FileGDBs). All layers will be auto-mapped to the selected schema fields and returned as a ZIP."
    )

    auto_zip = st.file_uploader("Upload ZIP of equipment data", type=["zip"], key="map_auto_zip")
    if auto_zip is not None:
        schema_files = list_reference_workbooks()
        if not schema_files:
            st.error("No reference workbooks found in reference_data.")
        else:
            schema_label_auto = st.selectbox("Schema workbook (auto)", list(schema_files.keys()), index=0, key="schema_wb_auto")
            schema_path_auto = schema_files[schema_label_auto]
            schema_excel_auto = pd.ExcelFile(schema_path_auto)
            schema_sheet_auto = st.selectbox("Schema sheet (auto)", schema_excel_auto.sheet_names, key="schema_sheet_auto")

            equipment_options_auto = list_schema_equipments(schema_path_auto, schema_sheet_auto)
            if normalize_for_compare(schema_sheet_auto) == normalize_for_compare("Electric device"):
                equipment_options_auto = ELECTRIC_DEVICE_EQUIPMENT
            if not equipment_options_auto:
                st.error("No equipment entries found in the schema sheet.")
            else:
                default_equip_idx_auto = 0
                equipment_name_auto = st.selectbox(
                    "Equipment/device (auto; used as fallback when no direct match)",
                    equipment_options_auto,
                    index=default_equip_idx_auto,
                    key="schema_equipment_auto",
                )

                mapping_threshold_auto = st.slider(
                    "Auto-mapping sensitivity (auto mode)",
                    min_value=0.0,
                    max_value=1.0,
                    value=0.35,
                    step=0.05,
                    key="map_threshold_auto",
                )
                keep_unmatched_auto = st.checkbox(
                    "Keep unmatched original columns (prefixed with orig_) in auto mode", value=True, key="keep_unmatched_auto"
                )

                if st.button("Run Automated Schema Mapping", key="run_auto_schema"):
                    status_msg = st.empty()
                    tmp_in = Path(tempfile.mkdtemp())
                    tmp_out = Path(tempfile.mkdtemp())
                    logs = []
                    try:
                        zip_in = tmp_in / "input.zip"
                        with open(zip_in, "wb") as f:
                            f.write(auto_zip.getbuffer())
                        with zipfile.ZipFile(zip_in, "r") as zf:
                            zf.extractall(tmp_in)

                        gpkg_paths = list(tmp_in.rglob("*.gpkg"))
                        # Support zipped FileGDBs inside the uploaded ZIP
                        gdb_zips = [p for p in tmp_in.rglob("*.zip") if p != zip_in]
                        for z in gdb_zips:
                            try:
                                with zipfile.ZipFile(z, "r") as zf:
                                    zf.extractall(z.parent)
                            except Exception:
                                continue
                        gdb_paths = list(tmp_in.rglob("*.gdb"))

                        status_msg.info(f"Unzipped. Found {len(gpkg_paths)} GPKG and {len(gdb_paths)} GDB paths. Starting mapping...")

                        if not gpkg_paths and not gdb_paths:
                            st.error("No GeoPackages or FileGDBs found inside the ZIP.")
                        else:
                            equip_map = load_gpkg_equipment_map()
                            # More aggressive acceptance for auto mode: use any suggested column (threshold handled by slider)
                            accept_threshold = 0.5
                            out_files = []

                            def process_layer(gdf_layer, driver, out_path, layer_name, schema_fields, type_map):
                                exclude_cols = {gdf_layer.geometry.name} if hasattr(gdf_layer, "geometry") else set()
                                suggested, score_map = fuzzy_map_columns_with_scores(
                                    list(gdf_layer.columns), schema_fields, threshold=mapping_threshold_auto, exclude=exclude_cols
                                )
                                norm_col_lookup = {normalize_for_compare(c): c for c in gdf_layer.columns}
                                n = len(gdf_layer)
                                def _na_series():
                                    return pd.Series([pd.NA] * n, index=gdf_layer.index)
                                out_cols = {}
                                for f in schema_fields:
                                    src = suggested.get(f)
                                    score = score_map.get(f, 0.0)
                                    chosen_src = None
                                    if src:
                                        resolved = norm_col_lookup.get(normalize_for_compare(src), src)
                                        if resolved in gdf_layer.columns:
                                            # Accept any suggested column; score filter already applied in fuzzy step
                                            chosen_src = resolved
                                    out_cols[f] = gdf_layer[chosen_src] if chosen_src else _na_series()
                                if keep_unmatched_auto:
                                    for col in gdf_layer.columns:
                                        if col not in suggested.values() and (not hasattr(gdf_layer, "geometry") or col != gdf_layer.geometry.name):
                                            out_cols[f"orig_{col}"] = gdf_layer[col]
                                geom_series = gdf_layer.geometry if hasattr(gdf_layer, "geometry") else None
                                for f in schema_fields:
                                    out_cols[f] = coerce_series_to_type(out_cols[f], type_map.get(f, ""))
                                out_layer = gpd.GeoDataFrame(out_cols, geometry=geom_series, crs=gdf_layer.crs)
                                out_layer = sanitize_gdf_for_gpkg(out_layer)
                                out_layer.to_file(out_path, driver=driver, layer=layer_name)

                            # Process GPKG files
                            gpkg_args = [
                                (
                                    gpkg,
                                    equipment_options_auto,
                                    equip_map,
                                    schema_path_auto,
                                    schema_sheet_auto,
                                    mapping_threshold_auto,
                                    keep_unmatched_auto,
                                    accept_threshold,
                                    str(tmp_out),
                                )
                                for gpkg in sorted(gpkg_paths)
                            ]
                            # Sequential mapping to avoid pool hangs in some environments
                            for args in gpkg_args:
                                out_path, log_msg = process_single_gpkg(args)
                                if out_path:
                                    out_files.append(out_path)
                                logs.append(log_msg)

                            # Process FileGDB folders
                            for gdb in sorted(gdb_paths):
                                try:
                                    layers = list_gpkg_layers(gdb)
                                    if not layers:
                                        logs.append(f"{gdb.name}: no layers found.")
                                        continue
                                    equipment_name = resolve_equipment_name(gdb.name, equipment_options_auto, equip_map)
                                    schema_fields_auto, type_map_auto = load_schema_fields(schema_path_auto, schema_sheet_auto, equipment_name)
                                    out_path = tmp_out / f"{gdb.name}.gdb"
                                    for lyr in layers:
                                        gdf_layer = gpd.read_file(gdb, layer=lyr)
                                        layer_name_out = derive_layer_name_from_filename(lyr)
                                        process_layer(gdf_layer, "FileGDB", out_path, layer_name_out, schema_fields_auto, type_map_auto)
                                    out_files.append(out_path)
                                    logs.append(f"{gdb.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'.")
                                except Exception as exc:
                                    logs.append(f"{gdb.name}: failed ({exc}).")

                            if out_files:
                                zip_out = shutil.make_archive(str(tmp_out / "auto_mapped"), "zip", root_dir=tmp_out, base_dir=".")
                                with open(zip_out, "rb") as f:
                                    data = f.read()
                                st.download_button(
                                    "Download Auto-Mapped Package (zip)",
                                    data=data,
                                    file_name="auto_mapped.zip",
                                    mime="application/zip",
                                    key="dl_auto_schema_zip",
                                )
                            status_msg.success(f"Mapping complete. Generated {len(out_files)} output files.")
                            st.text_area("Auto mapping log", value="\n".join(logs) if logs else "No logs.", height=220)
                    finally:
                        status_msg.empty()
                        shutil.rmtree(tmp_in, ignore_errors=True)
                        shutil.rmtree(tmp_out, ignore_errors=True)

if __name__ == "__main__":
    run_app()
