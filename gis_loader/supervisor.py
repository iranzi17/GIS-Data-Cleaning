import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from .config import DOMAIN_CODE_CACHE_FILE, NEW_DATA_DIR, REFERENCE_EXTENSIONS, SUPERVISOR_WORKBOOK_DIRS
from .equipment import PROTECTION_LAYOUT_DEVICES, SUPERVISOR_DEVICE_FALLBACKS
from .schema import coerce_series_to_type
from .text import clean_column_name, normalize_for_compare, normalize_value_for_compare


def domain_code_quality(domain_value: Any, code_value: Any) -> int:
    code_norm = normalize_value_for_compare(code_value)
    if not code_norm:
        return 0

    score = 1
    domain_norm = normalize_value_for_compare(domain_value)
    if domain_norm and code_norm != domain_norm:
        score += 2

    try:
        float(str(code_value).strip())
        score += 2
    except Exception:
        pass

    return score


def prefer_domain_code_value(
    domain_value: Any,
    existing_code: Any,
    candidate_code: Any,
    *,
    prefer_candidate_on_tie: bool = False,
) -> Any:
    existing_score = domain_code_quality(domain_value, existing_code)
    candidate_score = domain_code_quality(domain_value, candidate_code)

    if candidate_score == 0:
        return existing_code
    if existing_score == 0:
        return candidate_code
    if candidate_score > existing_score:
        return candidate_code
    if (
        prefer_candidate_on_tie
        and candidate_score == existing_score
        and normalize_value_for_compare(candidate_code) != normalize_value_for_compare(existing_code)
    ):
        return candidate_code
    return existing_code


@st.cache_data(show_spinner=False)
def load_domain_code_lookup() -> dict[str, Any]:
    """Build a global mapping of domain text -> domain code from supervisor workbooks."""
    lookup: dict[str, Any] = {}
    if DOMAIN_CODE_CACHE_FILE.exists():
        try:
            data = json.loads(DOMAIN_CODE_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                lookup.update(data)
        except Exception:
            pass

    def _is_new_data_path(path: Path) -> bool:
        try:
            return NEW_DATA_DIR in path.parents
        except Exception:
            return False

    workbooks: list[Path] = []
    for base_dir in SUPERVISOR_WORKBOOK_DIRS:
        if not base_dir.exists():
            continue
        for path in base_dir.glob("**/*"):
            if not path.is_file() or path.suffix.lower() not in REFERENCE_EXTENSIONS:
                continue
            if path.name.startswith("~$"):
                continue
            workbooks.append(path)
    if not workbooks:
        return lookup

    updated = False
    for workbook_path in sorted(workbooks):
        try:
            excel = pd.ExcelFile(workbook_path)
        except Exception:
            continue
        prefer_new_data = _is_new_data_path(workbook_path)
        for sheet in excel.sheet_names:
            try:
                raw = pd.read_excel(workbook_path, sheet_name=sheet, dtype=str, header=None)
            except Exception:
                continue
            if raw.empty or raw.shape[1] < 5:
                continue
            for _, row in raw.iterrows():
                domain_value = row.iloc[3]
                code_value = row.iloc[4]
                if pd.isna(domain_value) or pd.isna(code_value):
                    continue
                domain_norm = normalize_value_for_compare(domain_value)
                if not domain_norm:
                    continue
                existing = lookup.get(domain_norm)
                chosen = prefer_domain_code_value(
                    domain_value,
                    existing,
                    code_value,
                    prefer_candidate_on_tie=prefer_new_data,
                )
                if normalize_value_for_compare(chosen) != normalize_value_for_compare(existing):
                    lookup[domain_norm] = chosen
                    updated = True
    if updated:
        try:
            DOMAIN_CODE_CACHE_FILE.write_text(json.dumps(lookup, indent=2), encoding="utf-8")
        except Exception:
            pass
    return lookup


def parse_supervisor_device_table(workbook_path: Path, sheet_name: str, device_name: str) -> list[dict[str, Any]]:
    """
    Parse a supervisor-provided Electric device sheet into structured device instances.
    """
    raw = pd.read_excel(workbook_path, sheet_name=sheet_name, dtype=str, header=None)

    target_norm = normalize_for_compare(device_name)
    present_device_norms = set(raw.iloc[:, 0].dropna().map(normalize_for_compare)) if raw.shape[1] > 0 else set()
    target_norms = {target_norm}
    if target_norm not in present_device_norms:
        for candidate in SUPERVISOR_DEVICE_FALLBACKS.get(target_norm, []):
            candidate_norm = normalize_for_compare(candidate)
            if candidate_norm in present_device_norms:
                target_norms.add(candidate_norm)
                break

    is_protection = target_norm in PROTECTION_LAYOUT_DEVICES
    domain_code_map: dict[str, Any] = {}
    if raw.shape[1] > 4:
        for _, row in raw.iterrows():
            domain_value = row.iloc[3]
            code_value = row.iloc[4]
            if pd.isna(domain_value) or pd.isna(code_value):
                continue
            domain_norm = normalize_value_for_compare(domain_value)
            if domain_norm:
                existing = domain_code_map.get(domain_norm)
                domain_code_map[domain_norm] = prefer_domain_code_value(domain_value, existing, code_value)

    global_domain_map = load_domain_code_lookup()
    for key, value in global_domain_map.items():
        existing = domain_code_map.get(key)
        domain_code_map[key] = prefer_domain_code_value(key, existing, value)

    instances: list[dict[str, Any]] = []
    current_fields: dict[str, Any] | None = None
    type_map_device: dict[str, str] = {}
    current_domain_log: list[dict[str, Any]] = []

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
            norm = normalize_for_compare(text)
            if norm in {"notexisting", "notexist", "notavailable"}:
                return True
            if "locatedinthepowerplant" in norm or "locatedinpowerplant" in norm:
                return True
        return False

    def _extract_value(row: pd.Series, dtype: str, field_name: str) -> tuple[Any, dict[str, Any] | None]:
        def _looks_like_unit_value(value: Any) -> bool:
            if value is None:
                return False
            text = str(value)
            if text.strip() == "":
                return False
            has_digit = any(ch.isdigit() for ch in text)
            has_alpha = any(ch.isalpha() for ch in text)
            return has_digit and has_alpha

        value = row.iloc[3] if len(row) > 3 else pd.NA
        domain_code = row.iloc[4] if len(row) > 4 else pd.NA

        norm_type = normalize_for_compare(dtype or "")
        is_numeric = any(
            token in norm_type
            for token in ("int", "integer", "long", "short", "bigint", "smallint", "double", "float", "decimal", "real", "number")
        )

        if not _is_blank(domain_code):
            explicit_norm = normalize_value_for_compare(domain_code)
            domain_norm = normalize_value_for_compare(value)
            remapped = None

            if explicit_norm:
                mapped_from_code = domain_code_map.get(explicit_norm)
                if mapped_from_code is not None and normalize_value_for_compare(mapped_from_code) != explicit_norm:
                    remapped = mapped_from_code

            if remapped is None and domain_norm and explicit_norm == domain_norm:
                mapped_from_domain = domain_code_map.get(domain_norm)
                if mapped_from_domain is not None and normalize_value_for_compare(mapped_from_domain) != explicit_norm:
                    remapped = mapped_from_domain

            if remapped is not None and not _is_blank(remapped):
                return remapped, {
                    "field": field_name,
                    "domain": value,
                    "code": remapped,
                    "source": "mapped_explicit",
                }
            return domain_code, {
                "field": field_name,
                "domain": value,
                "code": domain_code,
                "source": "explicit",
            }

        if not _is_blank(value):
            domain_norm = normalize_value_for_compare(value)
            mapped = domain_code_map.get(domain_norm)
            if mapped is not None and not _is_blank(mapped):
                return mapped, {
                    "field": field_name,
                    "domain": value,
                    "code": mapped,
                    "source": "mapped",
                }
        if is_numeric and not _is_blank(value) and _looks_like_unit_value(value):
            return value, None
        if not _is_blank(value):
            return value, None

        if len(row) > 3:
            for candidate in row.iloc[3:]:
                if not _is_blank(candidate):
                    return candidate, None
        return pd.NA, None

    def _get_by_alias(fields: dict[str, Any], aliases: list[str]) -> Any:
        lookup = {normalize_for_compare(key): key for key in fields}
        for alias in aliases:
            key = lookup.get(normalize_for_compare(alias))
            if key is not None:
                return fields.get(key)
        return None

    def _finalize_instance(fields: dict[str, Any], order: list[str]) -> None:
        if not fields:
            return
        if all(_is_blank(value) for value in fields.values()):
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
                "circuitbreakerid",
                "circuitbreaker_id",
                "circuit breaker id",
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
                "type_map": type_map_device.copy(),
                "domain_log": current_domain_log.copy(),
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

        if dev_norm in target_norms:
            if current_fields is not None and current_fields:
                _finalize_instance(current_fields, current_order)
            current_fields = {}
            current_order = []
            current_domain_log = []
        elif pd.notna(dev_cell):
            if current_fields is not None and current_fields:
                _finalize_instance(current_fields, current_order)
            current_fields = None
            current_order = []
            current_domain_log = []

        if current_fields is None:
            continue
        if row_blank:
            continue

        field = row.iloc[1]
        if pd.isna(field):
            continue
        field_clean = clean_column_name(field)
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

        type_map_device[field_clean] = type_str
        value, log_entry = _extract_value(row, type_str, field_clean)
        if log_entry:
            current_domain_log.append(log_entry)
        series_val = pd.Series([value])
        coerced = coerce_series_to_type(series_val, type_str).iloc[0]
        current_fields[field_clean] = coerced
        if field_clean not in current_order:
            current_order.append(field_clean)

    if current_fields is not None and current_fields:
        _finalize_instance(current_fields, current_order)

    return instances
