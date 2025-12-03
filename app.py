import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
import unicodedata
import statistics
import difflib
import re
import json

import geopandas as gpd
import pandas as pd
import streamlit as st

# =====================================================================
# PATHS
# =====================================================================
BASE_DIR = Path(__file__).parent
REFERENCE_DATA_DIR = BASE_DIR / "reference_data"
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
    mapped = equip_map.get(norm_file)
    if mapped and mapped in equipment_options:
        return mapped
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

    out.columns = new_cols

    for col in out.columns:
        if col == geometry_name:
            continue
        series = ensure_valid_gpkg_dtypes(out[col])
        mask = pd.isna(series)
        if mask.any() and not pd.api.types.is_numeric_dtype(series):
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
                        substation_name = rel_parts[0] if len(rel_parts) > 1 else gpkg_path.stem
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
                                target_norm = normalize_value_for_compare(substation_name)
                                filtered_df = df_sheet.loc[(norm_col == target_norm).fillna(False)].copy()
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
                            for gpkg in sorted(gpkg_paths):
                                try:
                                    layers = list_gpkg_layers(gpkg)
                                    if not layers:
                                        logs.append(f"{gpkg.name}: no layers found.")
                                        continue
                                    equipment_name = resolve_equipment_name(gpkg.name, equipment_options_auto, equip_map)
                                    schema_fields_auto, type_map_auto = load_schema_fields(schema_path_auto, schema_sheet_auto, equipment_name)
                                    out_path = tmp_out / gpkg.name
                                    if out_path.exists():
                                        out_path.unlink()
                                    for lyr in layers:
                                        gdf_layer = gpd.read_file(gpkg, layer=lyr)
                                        layer_name_out = derive_layer_name_from_filename(lyr)
                                        process_layer(gdf_layer, "GPKG", out_path, layer_name_out, schema_fields_auto, type_map_auto)
                                    out_files.append(out_path)
                                    logs.append(f"{gpkg.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'.")
                                except Exception as exc:
                                    logs.append(f"{gpkg.name}: failed ({exc}).")

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
                            st.text_area("Auto mapping log", value="\n".join(logs) if logs else "No logs.", height=220)
                    finally:
                        shutil.rmtree(tmp_in, ignore_errors=True)
                        shutil.rmtree(tmp_out, ignore_errors=True)

if __name__ == "__main__":
    run_app()
