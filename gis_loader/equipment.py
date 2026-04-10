import difflib
import re
from pathlib import Path

from .data_sources import get_reference_columns, load_file_aliases
from .text import normalize_for_compare

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
    normalize_for_compare("TELECOM"): "Optical Telecommunication Equipment (Telecom)",
    normalize_for_compare("TELECOM_SDH"): "Optical Telecommunication Equipment (Telecom)",
    normalize_for_compare("TELECOM_ODF"): "ODF",
    normalize_for_compare("ODF"): "ODF",
    normalize_for_compare("OPTICAL TELECOMMUNICATION EQUIPMENT (TELECOM)"): "Optical Telecommunication Equipment (Telecom)",
    normalize_for_compare("DISTRIBUTION TRANSFORMER"): "Distribution Transformer",
    normalize_for_compare("DIST TRANSFORMER"): "Distribution Transformer",
    normalize_for_compare("AUX TRANSFORMER"): "Distribution Transformer",
    normalize_for_compare("CONTROL AND PROTECTION PANELS"): "Control and Protection Panels",
    normalize_for_compare("CB_INDOOR_SWITCH_GEAR"): "Indoor Circuit Breaker/30kv/15kb",
    normalize_for_compare("CT_INDOOR_SWITCH_GEAR"): "Indoor Current Transformer",
    normalize_for_compare("VT_INDOOR_SWITCH_GEAR"): "Indoor Voltage Transformer",
    normalize_for_compare("INDOOR_SWITCH_GEAR_TABLE"): "MV Switch gear",
}

# Supervisor sheets are not always consistent with device labels.
# Use these fallbacks only when an exact device block is missing.
SUPERVISOR_DEVICE_FALLBACKS = {
    normalize_for_compare("Transformer Bay"): [
        "Power cable to transformer",
        "Power cable to transfomer",
        "Transformer_Bay",
        "Transformer Bay",
        "Transformer",
    ],
}

PROTECTION_LAYOUT_DEVICES = {
    normalize_for_compare("Distance Protection"),
    normalize_for_compare("Transformer Protection"),
    normalize_for_compare("Line Overcurrent Protection"),
}

_FUZZY_ALIAS_MAP = {
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

_FUZZY_ALIAS_MAP_FOR_SCORES = {
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


def _merged_alias_map(base_alias: dict[str, list[str]]) -> dict[str, list[str]]:
    alias_map = {key: list(values) for key, values in base_alias.items()}
    file_aliases = load_file_aliases()
    for key, values in file_aliases.items():
        alias_map.setdefault(key, [])
        alias_map[key].extend([value for value in values if value not in alias_map[key]])
    return alias_map


def _tokenize(text: str) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(text).lower())
    return {token for token in cleaned.split() if token}


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
    return {variant for variant in variants if variant}


def _build_target_meta(target_fields: list[str], base_alias: dict[str, list[str]]) -> dict[str, dict[str, object]]:
    alias_map = _merged_alias_map(base_alias)
    norm_target = {normalize_for_compare(target): target for target in target_fields}
    alias_norm = {normalize_for_compare(key): [normalize_for_compare(value) for value in values] for key, values in alias_map.items()}

    dynamic_alias: dict[str, set[str]] = {norm_target_key: set() for norm_target_key in norm_target}
    ref_cols = get_reference_columns()
    for col in ref_cols:
        norm_col = normalize_for_compare(col)
        tokens_col = _tokenize(col)
        best_target = None
        best_score = 0.0
        for norm_target_key in norm_target:
            score = difflib.SequenceMatcher(None, norm_col, norm_target_key).ratio()
            if norm_col and norm_target_key and (norm_col in norm_target_key or norm_target_key in norm_col):
                score = max(score, 0.9)
            if tokens_col and _tokenize(norm_target_key):
                overlap = len(tokens_col & _tokenize(norm_target_key)) / max(len(tokens_col | _tokenize(norm_target_key)), 1)
                score = max(score, overlap)
            if score > best_score:
                best_score = score
                best_target = norm_target_key
        if best_target and best_score >= 0.8:
            dynamic_alias.setdefault(best_target, set()).add(norm_col)

    return {
        target_name: {
            "norm": norm_target_key,
            "variants": _variants(norm_target_key),
            "tokens": _tokenize(target_name),
            "aliases": set(alias_norm.get(norm_target_key, [])) | dynamic_alias.get(norm_target_key, set()),
        }
        for norm_target_key, target_name in norm_target.items()
    }


def _compute_fuzzy_mapping(
    source_cols: list[str],
    target_fields: list[str],
    threshold: float,
    exclude: set[str] | None,
    base_alias: dict[str, list[str]],
) -> tuple[dict[str, str], dict[str, float]]:
    result: dict[str, str] = {}
    result_scores: dict[str, float] = {}
    exclude = exclude or set()
    target_meta = _build_target_meta(target_fields, base_alias)

    for src in source_cols:
        if src in exclude:
            continue
        norm_src = normalize_for_compare(src)
        src_variants = _variants(norm_src)
        src_tokens = _tokenize(src)
        best_target = None
        best_score = threshold
        for target_name, meta in target_meta.items():
            score = 0.0
            aliases = meta["aliases"]
            variants = meta["variants"]
            tokens = meta["tokens"]
            if aliases and any(variant in aliases for variant in src_variants):
                score = max(score, 0.97)
            for source_variant in src_variants:
                for target_variant in variants:
                    if not source_variant and not target_variant:
                        continue
                    ratio = difflib.SequenceMatcher(None, source_variant, target_variant).ratio()
                    if source_variant and target_variant and (source_variant in target_variant or target_variant in source_variant):
                        ratio = max(ratio, 0.92)
                    score = max(score, ratio)
            if src_tokens and tokens:
                overlap = len(src_tokens & tokens) / max(len(src_tokens | tokens), 1)
                if overlap:
                    token_score = overlap + (0.05 if overlap == 1 else 0)
                    score = max(score, token_score)
            score = min(score, 1.0)
            if score > best_score or (best_target is None and score >= threshold) or (
                abs(score - best_score) < 1e-6 and best_target and len(target_name) > len(best_target)
            ):
                best_target = target_name
                best_score = score
        if best_target:
            prev = result_scores.get(best_target, -1)
            if (
                best_target not in result
                or best_score > prev + 1e-6
                or (abs(best_score - prev) < 1e-6 and len(src) < len(result.get(best_target, src + "x")))
            ):
                result[best_target] = src
                result_scores[best_target] = best_score

    return result, result_scores


def resolve_equipment_name(file_name: str, equipment_options: list[str], equip_map: dict[str, str]) -> str:
    """Pick equipment/device name for a given file using explicit map then similarity."""
    norm_file = normalize_for_compare(Path(file_name).stem)
    if "earthingtransformer" in norm_file or ("earthing" in norm_file and "transformer" in norm_file):
        return "Earthing Transformer" if equipment_options else "Earthing Transformer"

    override = FILE_DEVICE_OVERRIDES.get(norm_file)
    if override:
        if override in equipment_options:
            return override
        try:
            best = difflib.get_close_matches(
                normalize_for_compare(override),
                [normalize_for_compare(option) for option in equipment_options],
                n=1,
                cutoff=0.6,
            )
            if best:
                match_norm = best[0]
                for option in equipment_options:
                    if normalize_for_compare(option) == match_norm:
                        return option
        except Exception:
            pass

    norm_file_sub = norm_file
    if "powertransformer" in norm_file_sub or "power_transformer" in norm_file_sub or "powertransfomer" in norm_file_sub:
        for preferred in ("Power Transformer", "Power Transformer/ Stepup Transformer"):
            for option in equipment_options:
                if normalize_for_compare(option) == normalize_for_compare(preferred):
                    return option
        return "Power Transformer" if equipment_options else ""
    if (
        "distributiontransformer" in norm_file_sub
        or "distribution_transformer" in norm_file_sub
        or "disttransformer" in norm_file_sub
        or "dist_transformer" in norm_file_sub
        or "auxtransformer" in norm_file_sub
        or "aux_transformer" in norm_file_sub
    ):
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Distribution Transformer"):
                return option
        return "Distribution Transformer" if equipment_options else ""
    if "odf" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("ODF"):
                return option
        return "ODF" if equipment_options else ""
    if "telecom" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Optical Telecommunication Equipment (Telecom)"):
                return option
        return "Optical Telecommunication Equipment (Telecom)" if equipment_options else ""
    if "control" in norm_file_sub and "protection" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Control and Protection Panels"):
                return option
        return "Control and Protection Panels" if equipment_options else ""
    if "cbindoor" in norm_file_sub or "cb_indoor_switch" in norm_file_sub or "indoorcircuitbreaker" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Indoor Circuit Breaker/30kv/15kb"):
                return option
        return "Indoor Circuit Breaker/30kv/15kb" if equipment_options else ""
    if "ctindoor" in norm_file_sub or "ct_indoor_switch" in norm_file_sub or "indoorcurrenttransformer" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Indoor Current Transformer"):
                return option
        return "Indoor Current Transformer" if equipment_options else ""
    if (
        "vtindoor" in norm_file_sub
        or "vt_indoor_switch" in norm_file_sub
        or "vtindooor" in norm_file_sub
        or ("vt" in norm_file_sub and ("switchgear" in norm_file_sub or "switch_gear" in norm_file_sub))
        or "indoorvoltagetransformer" in norm_file_sub
    ):
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("Indoor Voltage Transformer"):
                return option
        return "Indoor Voltage Transformer" if equipment_options else ""
    if "disconnector" in norm_file_sub:
        for option in equipment_options:
            if normalize_for_compare(option) == normalize_for_compare("High Voltage Switch/High Voltage Switch"):
                return option
        return "High Voltage Switch/High Voltage Switch" if equipment_options else ""

    mapped = equip_map.get(norm_file)
    if mapped:
        mapped_norm = normalize_for_compare(mapped)
        if mapped_norm in {
            normalize_for_compare("Power Transformer/ Stepup Transformer"),
            normalize_for_compare("Power Transformer"),
        }:
            for preferred in ("Power Transformer", "Power Transformer/ Stepup Transformer"):
                for option in equipment_options:
                    if normalize_for_compare(option) == normalize_for_compare(preferred):
                        return option
        if mapped_norm in SUPERVISOR_DEVICE_FALLBACKS:
            return mapped
        if mapped in equipment_options:
            return mapped
        try:
            best = difflib.get_close_matches(
                normalize_for_compare(mapped),
                [normalize_for_compare(option) for option in equipment_options],
                n=1,
                cutoff=0.6,
            )
            if best:
                match_norm = best[0]
                for option in equipment_options:
                    if normalize_for_compare(option) == match_norm:
                        return option
        except Exception:
            pass

    try:
        best = difflib.get_close_matches(
            norm_file,
            [normalize_for_compare(option) for option in equipment_options],
            n=1,
            cutoff=0.5,
        )
        if best:
            match_norm = best[0]
            for option in equipment_options:
                if normalize_for_compare(option) == match_norm:
                    return option
    except Exception:
        pass
    return equipment_options[0] if equipment_options else ""


def fuzzy_map_columns(
    source_cols: list[str],
    target_fields: list[str],
    threshold: float = 0.6,
    exclude: set[str] | None = None,
) -> dict[str, str]:
    """Return mapping target_field -> source_col using rich fuzzy/alias logic."""
    mapping, _ = _compute_fuzzy_mapping(source_cols, target_fields, threshold, exclude, _FUZZY_ALIAS_MAP)
    return mapping


def fuzzy_map_columns_with_scores(
    source_cols: list[str],
    target_fields: list[str],
    threshold: float = 0.6,
    exclude: set[str] | None = None,
) -> tuple[dict[str, str], dict[str, float]]:
    """Variant of fuzzy_map_columns that also returns the best score per target."""
    return _compute_fuzzy_mapping(source_cols, target_fields, threshold, exclude, _FUZZY_ALIAS_MAP_FOR_SCORES)
