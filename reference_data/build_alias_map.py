"""
Utility script to build an alias_map.json from reference GeoPackages + Electric device sheet.
Run:
  python build_alias_map.py
"""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
ALIAS_FILE = BASE_DIR / "alias_map.json"
SCHEMA_PATH = BASE_DIR / "Substation Fields.xlsx"


def normalize_for_compare(name: str | None) -> str:
    if name is None:
        return ""
    return "".join(str(name).lower().split()).translate(str.maketrans("", "", " -_,./()\\")).strip()


def list_gpkg_layers(path: Path) -> list[str]:
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


def collect_target_fields() -> list[str]:
    df = pd.read_excel(SCHEMA_PATH, sheet_name="Electric device", dtype=str, header=None)
    df.iloc[:, 0] = df.iloc[:, 0].ffill()
    # assume field column is 1
    fields = df.iloc[:, 1].dropna().tolist()
    # keep order, remove empties
    seen = set()
    ordered = []
    for f in fields:
        if f and f not in seen:
            seen.add(f)
            ordered.append(f)
    return ordered


def collect_source_columns() -> list[str]:
    cols: set[str] = set()
    for gpkg in BASE_DIR.glob("*.gpkg"):
        for lyr in list_gpkg_layers(gpkg):
            try:
                gdf = gpd.read_file(gpkg, layer=lyr, rows=1)
                cols.update(gdf.columns)
            except Exception:
                continue
    return list(cols)


def build_alias_map(target_fields: list[str], source_cols: list[str], threshold: float = 0.6) -> dict[str, list[str]]:
    norm_target = {normalize_for_compare(t): t for t in target_fields}
    alias_map: dict[str, set[str]] = {t: set() for t in target_fields}

    for src in source_cols:
        norm_src = normalize_for_compare(src)
        best_t = None
        best_score = threshold
        for nt, tname in norm_target.items():
            if not norm_src and not nt:
                continue
            score = 0
            min_len = max(len(norm_src), len(nt))
            if min_len:
                overlap = len(set(norm_src) & set(nt)) / min_len
                score = overlap
            if norm_src and nt and (norm_src in nt or nt in norm_src):
                score = max(score, 0.9)
            if score > best_score or (abs(score - best_score) < 1e-6 and best_t and len(tname) < len(best_t)):
                best_t = tname
                best_score = score
        if best_t:
            alias_map[best_t].add(src)
    return {t: sorted(list(vals)) for t, vals in alias_map.items() if vals}


def main() -> None:
    targets = collect_target_fields()
    sources = collect_source_columns()
    alias_map = build_alias_map(targets, sources)
    ALIAS_FILE.write_text(json.dumps(alias_map, indent=2), encoding="utf-8")
    print(f"Wrote {len(alias_map)} targets to {ALIAS_FILE}")


if __name__ == "__main__":
    main()
