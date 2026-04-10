import difflib
import json
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from .config import (
    ALIAS_FILE,
    ELECTRIC_DEVICE_EQUIPMENT,
    GPKG_EQUIP_MAP_FILE,
    MAPPING_CACHE_FILE,
    NEW_DATA_DIR,
    REFERENCE_DATA_DIR,
    REFERENCE_EXTENSIONS,
    SUPERVISOR_WORKBOOK_DIRS,
)
from .text import normalize_for_compare, normalize_value_for_compare

_REFERENCE_ALIAS_COLUMNS: list[str] | None = None
_FILE_ALIAS_CACHE: dict[str, list[str]] | None = None
_GPKG_EQUIP_MAP: dict[str, str] | None = None
_MAPPING_CACHE: dict[str, dict[str, str]] | None = None
_EXCEL_FILE_CACHE: dict[str, pd.ExcelFile] = {}


@lru_cache(maxsize=1)
def list_reference_workbooks() -> dict[str, Path]:
    """Return mapping of display label -> workbook path for supported extensions."""
    workbooks: dict[str, Path] = {}
    if REFERENCE_DATA_DIR.exists():
        for path in sorted(REFERENCE_DATA_DIR.glob("**/*")):
            if path.is_file() and path.suffix.lower() in REFERENCE_EXTENSIONS:
                label = path.relative_to(REFERENCE_DATA_DIR).as_posix()
                workbooks[label] = path
    return workbooks


@lru_cache(maxsize=1)
def list_supervisor_workbooks() -> dict[str, Path]:
    """Return mapping of display label -> supervisor workbook path."""
    workbooks: dict[str, Path] = {}
    for base_dir in SUPERVISOR_WORKBOOK_DIRS:
        if not base_dir.exists():
            continue
        for path in sorted(base_dir.glob("**/*")):
            if not path.is_file() or path.suffix.lower() not in REFERENCE_EXTENSIONS:
                continue
            if path.name.startswith("~$"):
                continue
            rel = path.relative_to(base_dir).as_posix()
            label = f"{base_dir.name}/{rel}" if rel != "." else base_dir.name
            workbooks[label] = path
    return workbooks


def get_file_name(file_obj: Any) -> str:
    """Return a stable display name for a file-like object or Path."""
    if isinstance(file_obj, (Path, str)):
        return Path(file_obj).name
    return getattr(file_obj, "name", "")


def coerce_gpkg_path(file_obj: Any) -> Path | None:
    """Return a filesystem path for a GPKG file-like object or Path."""
    if file_obj is None:
        return None
    if isinstance(file_obj, (Path, str)):
        try:
            path = Path(file_obj)
            return path if path.exists() else None
        except Exception:
            return None
    data = None
    try:
        data = file_obj.getbuffer()
    except Exception:
        try:
            data = file_obj.read()
        except Exception:
            data = None
    if data is None:
        return None
    try:
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
            tmp.write(data)
            return Path(tmp.name)
    except Exception:
        return None


def normalize_substation_key(name: str) -> str:
    """Normalize substation/workbook labels for loose matching."""
    norm = normalize_value_for_compare(name)
    if not norm:
        return ""
    tokens = [token for token in norm.split() if token not in {"data", "substation"}]
    return " ".join(tokens).strip()


def build_supervisor_workbook_index(workbooks: dict[str, Path]) -> dict[str, tuple[str, Path]]:
    """Build normalized lookup for supervisor workbooks, preferring New Data when duplicated."""
    index: dict[str, tuple[str, Path]] = {}

    def _is_new_data_label(label: str, path: Path) -> bool:
        try:
            if normalize_for_compare(NEW_DATA_DIR.name) in normalize_for_compare(label):
                return True
        except Exception:
            pass
        try:
            return NEW_DATA_DIR in path.parents
        except Exception:
            return False

    for label, path in workbooks.items():
        stem = Path(label).stem
        norm = normalize_substation_key(stem)
        if norm and norm not in index:
            index[norm] = (label, path)
        elif norm:
            existing_label, existing_path = index[norm]
            if _is_new_data_label(label, path) and not _is_new_data_label(existing_label, existing_path):
                index[norm] = (label, path)
    return index


def resolve_supervisor_workbook_for_substation(
    substation_name: str,
    workbooks: dict[str, Path],
    index: dict[str, tuple[str, Path]] | None = None,
) -> tuple[str | None, Path | None]:
    """Return (label, path) for the best-matching supervisor workbook."""
    if not workbooks:
        return None, None
    if index is None:
        index = build_supervisor_workbook_index(workbooks)
    target = normalize_substation_key(substation_name)
    if not target:
        return None, None
    if target in index:
        return index[target]
    for key, value in index.items():
        if key and (key in target or target in key):
            return value
    try:
        best = difflib.get_close_matches(target, list(index.keys()), n=1, cutoff=0.6)
        if best:
            return index[best[0]]
    except Exception:
        pass
    return None, None


def pick_supervisor_sheet(excel_file: pd.ExcelFile) -> str | None:
    """Pick a default supervisor sheet for a workbook."""
    if not excel_file.sheet_names:
        return None
    for sheet in excel_file.sheet_names:
        if normalize_for_compare(sheet) == normalize_for_compare("Electric device"):
            return sheet

    def _looks_like_device_sheet(sheet_name: str) -> bool:
        try:
            preview = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, dtype=str, nrows=8)
        except Exception:
            return False
        if preview.empty or preview.shape[1] < 2:
            return False
        for _, row in preview.iterrows():
            v0 = row.iloc[0]
            v1 = row.iloc[1]
            if normalize_for_compare(v0) == "device" and normalize_for_compare(v1) == "field":
                return True
        return False

    for sheet in excel_file.sheet_names:
        if _looks_like_device_sheet(sheet):
            return sheet
    return excel_file.sheet_names[0]


@lru_cache(maxsize=256)
def list_gpkg_layers(path: Path | str) -> list[str]:
    """List layers inside a GeoPackage or FileGDB path."""
    path = Path(path)
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


def get_reference_columns() -> list[str]:
    """Collect column names from reference GeoPackages to enrich fuzzy aliases."""
    global _REFERENCE_ALIAS_COLUMNS
    if _REFERENCE_ALIAS_COLUMNS is not None:
        return _REFERENCE_ALIAS_COLUMNS
    cols: set[str] = set()
    try:
        for path in REFERENCE_DATA_DIR.glob("*.gpkg"):
            for layer in list_gpkg_layers(path):
                try:
                    gdf = gpd.read_file(path, layer=layer, rows=1)
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
                _FILE_ALIAS_CACHE = {key: value if isinstance(value, list) else [] for key, value in data.items()}
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
        "transformers": "Power Transformer/ Stepup Transformer",
        "distribution transformer": "Distribution Transformer",
        "distribution_transformer": "Distribution Transformer",
        "dist transformer": "Distribution Transformer",
        "dist_transformer": "Distribution Transformer",
        "aux transformer": "Distribution Transformer",
        "aux_transformer": "Distribution Transformer",
        "voltage transformer": "Voltage Transformer",
        "vt indor switchgear": "Indoor Voltage Transformer",
        "ups": "Uninterruptable power supply(UPS)",
        "trans_system prot1": "Distance Protection",
        "telecom": "Optical Telecommunication Equipment (Telecom)",
        "odf": "ODF",
        "control and protection panels": "Control and Protection Panels",
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
        "telecom_sdh": "Optical Telecommunication Equipment (Telecom)",
        "telecom_odf": "ODF",
        "highvoltage_line": "Line Bay",
        "transformer_bay": "Transformer Bay",
        "power_transformer": "Power Transformer/ Stepup Transformer",
        "powertransformer": "Power Transformer/ Stepup Transformer",
        "standby generator": "Standby Generator",
        "standby_generator": "Standby Generator",
        "cb_indoor_switch_gear": "Indoor Circuit Breaker/30kv/15kb",
        "ct_indoor_switch_gear": "Indoor Current Transformer",
        "vt_indoor_switch_gear": "Indoor Voltage Transformer",
        "vt_indo0or_switch_gear": "Indoor Voltage Transformer",
        "vt_indooor_switch_gear": "Indoor Voltage Transformer",
        "power_transfomer": "Power Transformer/ Stepup Transformer",
        "disconnector_switches": "High Voltage Switch/High Voltage Switch",
    }

    if GPKG_EQUIP_MAP_FILE.exists():
        try:
            data = json.loads(GPKG_EQUIP_MAP_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                loaded = {normalize_for_compare(key): str(value) for key, value in data.items()}
                default_map.update(loaded)
        except Exception:
            pass

    canon_map: dict[str, str] = {}
    for norm_key, value in default_map.items():
        target = value
        try:
            best = difflib.get_close_matches(
                normalize_for_compare(value),
                [normalize_for_compare(option) for option in ELECTRIC_DEVICE_EQUIPMENT],
                n=1,
                cutoff=0.5,
            )
            if best:
                match_norm = best[0]
                for option in ELECTRIC_DEVICE_EQUIPMENT:
                    if normalize_for_compare(option) == match_norm:
                        target = option
                        break
        except Exception:
            target = value
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
                _MAPPING_CACHE = {str(key): value if isinstance(value, dict) else {} for key, value in data.items()}
                return _MAPPING_CACHE
        except Exception:
            pass
    _MAPPING_CACHE = {}
    return _MAPPING_CACHE


def save_mapping_cache(cache: dict[str, dict[str, str]]) -> None:
    """Persist field mapping choices."""
    try:
        MAPPING_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception:
        pass


def cache_key_from_path(path: Path | str) -> str:
    """Stable string key for caching by filesystem path."""
    try:
        return str(Path(path).resolve())
    except Exception:
        return str(path)


def excel_key_from_file(excel_file: pd.ExcelFile) -> str:
    """Return the stable cache key associated with a cached workbook."""
    if hasattr(excel_file, "_cache_key"):
        return getattr(excel_file, "_cache_key")
    try:
        return cache_key_from_path(getattr(excel_file, "io", excel_file))
    except Exception:
        return str(excel_file)


def get_excel_file(workbook_path: Path) -> pd.ExcelFile:
    """Return cached pd.ExcelFile for a workbook path."""
    key = cache_key_from_path(workbook_path)
    cached = _EXCEL_FILE_CACHE.get(key)
    if cached is not None:
        return cached
    excel_file = pd.ExcelFile(workbook_path)
    setattr(excel_file, "_cache_key", key)
    _EXCEL_FILE_CACHE[key] = excel_file
    return excel_file
