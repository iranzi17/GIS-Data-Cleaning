from pathlib import Path

import geopandas as gpd
import pandas as pd

from .errors import DatasetReadError, MappingError
from .equipment import fuzzy_map_columns_with_scores, resolve_equipment_name
from .geopackage import derive_layer_name_from_filename, sanitize_gdf_for_gpkg
from .reporting import RunReport
from .schema import coerce_series_to_type, load_schema_fields
from .data_sources import list_gpkg_layers
from .text import normalize_for_compare
from .validation import (
    build_dataset_profile,
    build_field_mapping_rows,
    build_mapping_validations,
    build_output_validations,
)


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
    report = RunReport(workflow="automated_schema_mapping_file", title=str(Path(gpkg).name))
    try:
        gpkg = Path(gpkg)
        report.set_metadata(
            input_name=gpkg.name,
            schema_workbook=Path(schema_path_auto).name,
            schema_sheet=schema_sheet_auto,
            mapping_threshold=mapping_threshold_auto,
            keep_unmatched=keep_unmatched_auto,
            accept_threshold=accept_threshold,
        )
        report.add_artifact("input_dataset", gpkg.name, gpkg)
        layers = list_gpkg_layers(gpkg)
        if not layers:
            raise DatasetReadError(f"{gpkg.name}: no layers found.", details={"path": str(gpkg)})
        equipment_name = resolve_equipment_name(gpkg.name, equipment_options_auto, equip_map)
        schema_fields_auto, type_map_auto = load_schema_fields(schema_path_auto, schema_sheet_auto, equipment_name)
        out_path = Path(tmp_out_str) / gpkg.name
        if out_path.exists():
            out_path.unlink()
        for lyr in layers:
            gdf_layer = gpd.read_file(gpkg, layer=lyr)
            report.add_artifact(
                "input_layer",
                f"{gpkg.name}:{lyr}",
                details=build_dataset_profile(gdf_layer, name=gpkg.name, layer_name=lyr, source_format="gpkg"),
            )
            layer_name_out = derive_layer_name_from_filename(lyr)
            exclude_cols = {gdf_layer.geometry.name} if hasattr(gdf_layer, "geometry") else set()
            suggested, score_map = fuzzy_map_columns_with_scores(
                list(gdf_layer.columns), schema_fields_auto, threshold=mapping_threshold_auto, exclude=exclude_cols
            )
            mapping_rows = build_field_mapping_rows(
                list(gdf_layer.columns),
                schema_fields_auto,
                suggested,
                suggested_mapping=suggested,
                score_map=score_map,
                geometry_name=gdf_layer.geometry.name if hasattr(gdf_layer, "geometry") else None,
                low_confidence_threshold=accept_threshold,
            )
            report.add_section_rows(f"{gpkg.stem}_{lyr}_field_mapping", mapping_rows)
            for result in build_mapping_validations(mapping_rows, low_confidence_threshold=accept_threshold):
                report.add_validation_result(result)
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
            for result in build_output_validations(
                out_layer,
                expected_fields=schema_fields_auto,
                geometry_required=hasattr(gdf_layer, "geometry"),
                label=f"{gpkg.name}:{lyr}",
            ):
                report.add_validation_result(result)
            report.add_artifact(
                "output_layer",
                f"{gpkg.name}:{layer_name_out}",
                details=build_dataset_profile(out_layer, name=gpkg.name, layer_name=layer_name_out, source_format="gpkg"),
            )
        report.add_artifact("output_dataset", out_path.name, out_path, details={"layer_count": len(layers), "equipment_name": equipment_name})
        report.info("auto_schema_mapping_completed", f"{gpkg.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'.")
        return out_path, f"{gpkg.name}: mapped {len(layers)} layer(s) using equipment '{equipment_name}'.", report
    except (DatasetReadError, MappingError) as exc:
        report.exception(exc.code, exc, getattr(exc, "details", None))
        return None, f"{Path(gpkg).name}: failed ({exc}).", report
    except Exception as exc:
        report.exception("auto_schema_mapping_failed", exc, {"input_name": Path(gpkg).name})
        return None, f"{Path(gpkg).name}: failed ({exc}).", report
