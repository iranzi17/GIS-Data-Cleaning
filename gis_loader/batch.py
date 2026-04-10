from pathlib import Path

import geopandas as gpd
import pandas as pd

from .equipment import fuzzy_map_columns_with_scores, resolve_equipment_name
from .geopackage import derive_layer_name_from_filename, sanitize_gdf_for_gpkg
from .schema import coerce_series_to_type, load_schema_fields
from .data_sources import list_gpkg_layers
from .text import normalize_for_compare


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
            norm_col_lookup = {normalize_for_compare(col): col for col in gdf_layer.columns}
            n = len(gdf_layer)

            def _na_series():
                return pd.Series([pd.NA] * n, index=gdf_layer.index)

            out_cols = {}
            for field in schema_fields_auto:
                src = suggested.get(field)
                chosen_src = None
                if src:
                    resolved = norm_col_lookup.get(normalize_for_compare(src), src)
                    if resolved in gdf_layer.columns:
                        chosen_src = resolved
                out_cols[field] = gdf_layer[chosen_src] if chosen_src else _na_series()
            if keep_unmatched_auto:
                for col in gdf_layer.columns:
                    if col not in suggested.values() and (not hasattr(gdf_layer, "geometry") or col != gdf_layer.geometry.name):
                        out_cols[f"orig_{col}"] = gdf_layer[col]
            geom_series = gdf_layer.geometry if hasattr(gdf_layer, "geometry") else None
            for field in schema_fields_auto:
                out_cols[field] = coerce_series_to_type(out_cols[field], type_map_auto.get(field, ""))
            out_layer = gpd.GeoDataFrame(out_cols, geometry=geom_series, crs=gdf_layer.crs)
            out_layer = sanitize_gdf_for_gpkg(out_layer)
            out_layer.to_file(out_path, driver="GPKG", layer=layer_name_out)
        return out_path, f"{gpkg.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'."
    except Exception as exc:
        return None, f"{Path(gpkg).name}: failed ({exc})."
