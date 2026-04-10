import difflib
from pathlib import Path

import pandas as pd

from .schema import get_sheet_header
from .text import normalize_for_compare, normalize_value_for_compare

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
        "Line_Bay_Name",
        "Line Bay Name",
        "LineBayName",
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


def detect_best_sheet(excel_file: pd.ExcelFile, gdf_columns: list[str]) -> str | None:
    """Pick the workbook sheet whose cleaned header best matches the dataset columns."""
    best_sheet = None
    best_score = 0.0
    gdf_norm = {normalize_for_compare(col) for col in gdf_columns}
    for sheet in excel_file.sheet_names:
        header = get_sheet_header(excel_file, sheet)
        if not header:
            continue
        header_norm = {normalize_for_compare(cell) for cell in header if cell}
        overlap = len(gdf_norm & header_norm)
        denom = max(len(header_norm), 1)
        score = overlap / denom
        if score > best_score:
            best_score = score
            best_sheet = sheet
    return best_sheet


def select_sheet_for_gpkg(
    excel_file: pd.ExcelFile,
    gpkg_name: str,
    gdf_columns: list[str],
    auto_sheet: bool,
    fallback_sheet: str,
) -> str | None:
    """
    Choose the sheet for a given GeoPackage name using the manual map first,
    then optional auto-selection, then fallback.
    """
    norm = normalize_for_compare(Path(gpkg_name).stem)
    sheet_lookup = {normalize_for_compare(sheet): sheet for sheet in excel_file.sheet_names}

    candidates = GPKG_SHEET_MAP.get(norm, [])
    if candidates:
        for candidate in candidates:
            candidate_norm = normalize_for_compare(candidate)
            if candidate_norm in sheet_lookup:
                return sheet_lookup[candidate_norm]
        return None

    if auto_sheet:
        detected = detect_best_sheet(excel_file, gdf_columns)
        if detected:
            return detected
    return fallback_sheet


def detect_join_columns(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    geometry_name: str | None = None,
) -> tuple[str | None, str | None, int]:
    """
    Heuristic to find join columns between a spatial dataframe and a workbook dataframe.
    Prefers value overlap, then falls back to column-name similarity.
    """

    def _norm_series(series: pd.Series) -> pd.Series:
        return series.dropna().map(normalize_value_for_compare)

    left_candidates = [col for col in left_df.columns if col != geometry_name]
    right_candidates = list(right_df.columns)

    best = (None, None, 0, 0.0)
    for left_col in left_candidates:
        left_norm = set(_norm_series(left_df[left_col]))
        if not left_norm:
            continue
        for right_col in right_candidates:
            right_norm = set(_norm_series(right_df[right_col]))
            if not right_norm:
                continue
            inter = len(left_norm & right_norm)
            coverage = inter / max(len(right_norm), 1)
            if inter > best[2] or (inter == best[2] and coverage > best[3]):
                best = (left_col, right_col, inter, coverage)

    left_key, right_key, match_count, coverage = best
    if match_count > 0:
        return left_key, right_key, match_count

    best_similarity = (None, None, 0.0)
    for left_col in left_candidates:
        norm_left = normalize_for_compare(left_col)
        for right_col in right_candidates:
            norm_right = normalize_for_compare(right_col)
            ratio = difflib.SequenceMatcher(None, norm_left, norm_right).ratio()
            if ratio > best_similarity[2]:
                best_similarity = (left_col, right_col, ratio)
    if best_similarity[2] >= 0.6:
        return best_similarity[0], best_similarity[1], 0
    return None, None, 0


def preferred_match_columns(device_name: str) -> list[str]:
    """Return preferred match columns for specific devices when aligning supervisor rows."""
    norm = normalize_for_compare(device_name)
    preferences = {
        normalize_for_compare("Line Bay"): [
            "Line_Bay_Name",
            "Line Bay Name",
            "LineBayName",
            "LineBayID",
            "Line Bay ID",
            "Line_Bay_ID",
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
        normalize_for_compare("Distribution Transformer"): [
            "transfomerID",
            "TransformerID",
            "Transformer ID",
            "transfomer ID",
        ],
    }
    return preferences.get(norm, [])


def match_overrides_for_file(file_name: str) -> list[str]:
    """Return per-file preferred match columns based on known filename overrides."""
    norm = normalize_for_compare(Path(file_name).stem)
    return FILE_MATCH_OVERRIDES.get(norm, [])
