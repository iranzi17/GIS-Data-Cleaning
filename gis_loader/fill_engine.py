import tempfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from .config import LINE_BAY_LIBRARY_PATH
from .data_sources import coerce_gpkg_path as _coerce_gpkg_path
from .data_sources import get_file_name as _get_file_name
from .data_sources import list_gpkg_layers
from .equipment import PROTECTION_LAYOUT_DEVICES
from .fill import (
    ASPATIAL_DEVICES,
    BLOCK_ASSIGN_DEVICES,
    FillBatchDependencies,
    LINE_BAY_SPATIAL_DEVICES,
    PREFIX_GROUP_DEVICES,
    PROTECTION_LAYOUT_SPACING,
    SUBSTATION_FORCE_TYPES,
    SUBSTATION_PRESERVE_FIELDS,
    build_device_gdf_from_instances,
    build_device_table_from_instances,
    build_spatial_match_targets,
    ensure_name_fields_string,
    repeat_instances,
    split_instance_prefix_suffix,
)
from .geopackage import derive_layer_name_from_filename, sanitize_gdf_for_gpkg, write_aspatial_gpkg_layer
from .schema import coerce_series_to_type
from .spatial import (
    _extract_bay_name_from_row,
    _pick_line_bay_name_field,
    apply_line_bay_names,
    build_cabin_anchor_points,
    build_control_panel_polygons,
    build_lines_from_points_in_polygon,
    build_points_in_panel_polygons,
    build_protection_layout_points,
    collect_device_points_from_uploads,
    collect_device_polygons_from_uploads,
    collect_point_geometries_from_uploads,
    enrich_line_bay_reference_info,
    expand_geometries,
    group_indices_by_perp_gap,
    layout_points_in_cabins,
    load_line_bay_layer,
    load_template_layer,
    load_ups_anchor_and_crs,
    map_points_to_bays,
    order_indices_by_location,
    replace_line_name_ids,
    resolve_ups_anchor_point,
)
from .supervisor import parse_supervisor_device_table
from .text import normalize_for_compare, normalize_value_for_compare


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
    type_map: dict[str, str] | None = None,
    sup_wb_path: Path | None = None,
    sup_sheet: str | None = None,
    seq_assign_fallback: bool = True,
    control_panel_polygons: list[Any] | None = None,
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
                        "type_map": inst.get("type_map"),
                    }
                )
            else:
                seq_entries.append(
                    {"fields": inst if isinstance(inst, dict) else {}, "id": None, "name": None, "type_map": None}
                )

    type_map_local = dict(type_map) if isinstance(type_map, dict) else {}

    def _extract_type_map(instances: list[dict[str, Any]] | None) -> dict[str, str]:
        if not instances:
            return {}
        for inst in instances:
            tm = inst.get("type_map") if isinstance(inst, dict) else None
            if isinstance(tm, dict) and tm:
                return dict(tm)
        return {}

    if not type_map_local:
        type_map_local = _extract_type_map(seq_entries)

    file_name = _get_file_name(file_obj)

    if normalize_for_compare(device_name) in ASPATIAL_DEVICES:
        table_instances: list[dict[str, Any]] = []
        if seq_entries:
            for entry in seq_entries:
                fields = dict(entry.get("fields", {}) or {})
                table_instances.append(
                    {
                        "fields": fields,
                        "order": list(fields.keys()),
                        "type_map": entry.get("type_map") or type_map_local,
                    }
                )
        elif field_map:
            fields = dict(field_map)
            table_instances = [
                {
                    "fields": fields,
                    "order": list(field_order or fields.keys()),
                    "type_map": type_map_local,
                }
            ]
        elif sup_wb_path is not None and sup_sheet is not None:
            table_instances = parse_supervisor_device_table(sup_wb_path, sup_sheet, device_name)

        if not table_instances:
            raise ValueError(f"No entries found for device '{device_name}' in sheet '{sup_sheet}'.")

        out_df = build_device_table_from_instances(table_instances)
        if out_df.empty and len(out_df.columns) == 0:
            raise ValueError(f"No fields available for aspatial device '{device_name}'.")

        layer_name = layer_override or derive_layer_name_from_filename(file_name or device_name)
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
            out_path = Path(tmpout.name)
        write_aspatial_gpkg_layer(out_df, out_path, layer_name)
        return out_path, layer_name

    block_assign = normalize_for_compare(device_name) in BLOCK_ASSIGN_DEVICES
    strict_line_bay = normalize_for_compare(device_name) == normalize_for_compare("Line Bay")

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
    gpkg_path = _coerce_gpkg_path(file_obj)
    if gpkg_path is None:
        raise ValueError("Could not read the GeoPackage.")
    layers = list_gpkg_layers(gpkg_path)
    layer = layer_override or (layers[0] if layers else None)
    if not layer:
        raise ValueError("No layers found in the uploaded GeoPackage.")
    gdf_sup_local = gpd.read_file(gpkg_path, layer=layer)
    geom_name = gdf_sup_local.geometry.name if hasattr(gdf_sup_local, "geometry") else None
    geom_crs = gdf_sup_local.crs if hasattr(gdf_sup_local, "crs") else None
    preserve_cols: set[str] = set()
    preserve_type_map: dict[str, str] = {}
    preserve_all_cols = normalize_for_compare(device_name) == normalize_for_compare("Substation/Cabin")
    if normalize_for_compare(device_name) == normalize_for_compare("Substation/Cabin"):
        for col in gdf_sup_local.columns:
            norm_col = normalize_for_compare(col)
            if norm_col in SUBSTATION_PRESERVE_FIELDS:
                preserve_cols.add(col)
            if norm_col in SUBSTATION_FORCE_TYPES:
                preserve_type_map[norm_col] = SUBSTATION_FORCE_TYPES[norm_col]
    preserve_norms = {normalize_for_compare(c) for c in preserve_cols}

    def _is_preserved_field(field_name: Any) -> bool:
        return normalize_for_compare(field_name) in preserve_norms
    layout_applied = False
    if normalize_for_compare(device_name) in PROTECTION_LAYOUT_DEVICES and hasattr(gdf_sup_local, "geometry"):
        desired_count = len(gdf_sup_local)
        if seq_entries and len(seq_entries) > desired_count:
            desired_count = len(seq_entries)

        def _apply_layout_points(points: list[Any]) -> bool:
            nonlocal gdf_sup_local
            if not points or len(points) != desired_count:
                return False
            if desired_count > len(gdf_sup_local):
                extra = desired_count - len(gdf_sup_local)
                extra_rows = pd.DataFrame({col: [pd.NA] * extra for col in gdf_sup_local.columns})
                gdf_sup_local = pd.concat([gdf_sup_local, extra_rows], ignore_index=True)
                if geom_name:
                    gdf_sup_local = gpd.GeoDataFrame(
                        gdf_sup_local,
                        geometry=geom_name,
                        crs=geom_crs,
                    )
            gdf_sup_local = gdf_sup_local.copy()
            for idx_pt, pt in enumerate(points):
                try:
                    gdf_sup_local.geometry.iat[idx_pt] = pt
                except Exception:
                    continue
            return True

        panel_points = build_points_in_panel_polygons(control_panel_polygons, desired_count)
        if panel_points and _apply_layout_points(panel_points):
            layout_applied = True
        elif ups_anchor_info:
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
                layout_points = build_protection_layout_points(anchor, desired_count, spacing_val)
                if _apply_layout_points(layout_points):
                    layout_applied = True
    fm_local = field_map
    order_local = field_order or []
    if fm_local is None and match_column is None:
        if sup_wb_path is None or sup_sheet is None:
            raise ValueError("Supervisor workbook and sheet are required for parsing.")
        parsed = parse_supervisor_device_table(sup_wb_path, sup_sheet, device_name)
        if not parsed:
            raise ValueError(f"No entries found for device '{device_name}' in sheet '{sup_sheet}'.")
        # keep parsed instances available for fallback sequential assignment
        parsed_instances = parsed
        fm_local = parsed[0].get("fields", {})
        order_local = parsed[0].get("order", [])
        if not type_map_local:
            type_map_local = _extract_type_map(parsed)
    if fm_local is None and match_column is None:
        raise ValueError(f"No field values available for device '{device_name}'.")
    out_cols: dict[str, Any] = {}
    if geom_name:
        out_cols[geom_name] = gdf_sup_local.geometry
    n = len(gdf_sup_local)
    filled_fields: list[str] = []
    if match_column and match_column in gdf_sup_local.columns:
        out_cols[match_column] = gdf_sup_local[match_column].copy()
    if preserve_all_cols:
        for col in gdf_sup_local.columns:
            if col == geom_name:
                continue
            if col not in out_cols:
                out_cols[col] = gdf_sup_local[col].copy()

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
            if f == geom_name or _is_preserved_field(f):
                continue
            out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)

        # Preserve selected Substation fields from the uploaded layer.
        for col in preserve_cols:
            out_cols[col] = gdf_sup_local[col].copy()

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
                if f == geom_name or _is_preserved_field(f):
                    continue
                if f not in out_cols:
                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                out_cols[f].iat[idx_val] = fill_val

        # If single feature and nothing matched, fill with default or first instance.
        if matched_hits == 0 and n == 1 and not strict_line_bay:
            fallback_fields = default_fields
            if fallback_fields is None and instance_map:
                # take first instance_map entry
                first_payload = next(iter(instance_map.values()), (None, []))
                fallback_fields = first_payload[0]
            if fallback_fields:
                for f, val in fallback_fields.items():
                    if f == geom_name or _is_preserved_field(f):
                        continue
                    if f not in out_cols:
                        out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                    fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                    out_cols[f].iat[0] = fill_val
        # If multi-feature and no matches at all but we have defaults, fill all rows with defaults.
        if matched_hits == 0 and n > 1 and default_fields and not strict_line_bay:
            for f, val in default_fields.items():
                if f == geom_name or _is_preserved_field(f):
                    continue
                if f not in out_cols:
                    out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                out_cols[f] = pd.Series([fill_val] * n, index=gdf_sup_local.index)
        # If still no matches and sequential instances are provided, distribute them across rows.
        if matched_hits == 0 and seq_entries and not strict_line_bay:
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
                    if f == geom_name or _is_preserved_field(f):
                        continue
                    if f not in out_cols:
                        out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                    fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                    out_cols[f].iat[idx_row] = fill_val
                _maybe_fill_match_id(idx_row, entry)

        # If some rows remain unmatched, fill those rows using sequential instances (feeder-aware) without overwriting matched rows.
        if (not strict_line_bay) and (
            (seq_entries and len(matched_indices) < n) or (
                not seq_entries
                and 'parsed_instances' in locals()
                and len(parsed_instances) > 1
                and len(matched_indices) < n
                and seq_assign_fallback
            )
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
                            "type_map": inst.get("type_map"),
                        })
                    else:
                        seq_entries.append(
                            {"fields": inst if isinstance(inst, dict) else {}, "id": None, "name": None, "type_map": None}
                        )
                if not type_map_local:
                    type_map_local = _extract_type_map(seq_entries)
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
                    if f == geom_name or _is_preserved_field(f):
                        continue
                    if f not in out_cols:
                        out_cols[f] = pd.Series([pd.NA] * n, index=gdf_sup_local.index)
                    if pd.isna(out_cols[f].iat[idx_row]):
                        fill_val = val.iloc[0] if isinstance(val, pd.Series) else val
                        out_cols[f].iat[idx_row] = fill_val
                _maybe_fill_match_id(idx_row, entry)

        filled_fields = [f for f in out_cols.keys() if f != geom_name]
    else:
        if strict_line_bay:
            # Preserve existing attributes for Line Bay when no matching is available; avoid auto-fill.
            for col in gdf_sup_local.columns:
                if col == geom_name:
                    continue
                out_cols[col] = gdf_sup_local[col]
            filled_fields = [c for c in gdf_sup_local.columns if c != geom_name]
        elif preserve_cols and not preserve_all_cols:
            # Preserve selected Substation fields but allow other fields to fill normally.
            for col in preserve_cols:
                out_cols[col] = gdf_sup_local[col].copy()
            filled_fields = [f for f in out_cols.keys() if f != geom_name]
        elif seq_entries:
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
                    if f == geom_name or _is_preserved_field(f):
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
                if _is_preserved_field(f):
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

    if type_map_local:
        norm_type_lookup = {
            normalize_for_compare(k): v for k, v in type_map_local.items() if v is not None
        }
        for col_name, series in list(out_cols.items()):
            if col_name == geom_name:
                continue
            norm_col = normalize_for_compare(col_name)
            t_str = preserve_type_map.get(norm_col)
            if t_str is None:
                t_str = type_map_local.get(col_name)
            if t_str is None:
                t_str = norm_type_lookup.get(norm_col)
            if t_str:
                try:
                    out_cols[col_name] = coerce_series_to_type(series, t_str)
                except Exception:
                    pass

    keep_cols = filled_fields.copy()
    if match_column and match_column in out_cols:
        norm_keep = {normalize_for_compare(c) for c in keep_cols}
        if normalize_for_compare(match_column) not in norm_keep:
            keep_cols.append(match_column)
    for col in preserve_cols:
        if col not in keep_cols and col in out_cols:
            keep_cols.append(col)
    if geom_name and geom_name not in keep_cols:
        keep_cols.append(geom_name)

    if preserve_all_cols:
        for col in gdf_sup_local.columns:
            if col != geom_name and col in out_cols and col not in keep_cols:
                keep_cols.append(col)
    else:
        # Drop utility columns (e.g., Composite_ID) from the output.
        keep_cols = [c for c in keep_cols if normalize_for_compare(c) not in DROP_OUTPUT_COLUMNS]

    if preserve_all_cols:
        # Reorder attributes to match supervisor sheet order, then original column order.
        norm_out_lookup = {normalize_for_compare(c): c for c in out_cols.keys()}
        if order_local:
            sheet_order = order_local
        elif type_map_local:
            sheet_order = list(type_map_local.keys())
        else:
            sheet_order = []
        ordered_cols: list[str] = []
        # Ensure preserved Substation fields keep their preferred ordering.
        for pref in SUBSTATION_PRESERVE_ORDER:
            col = norm_out_lookup.get(normalize_for_compare(pref))
            if col and col not in ordered_cols:
                ordered_cols.append(col)
        for f in sheet_order:
            col = norm_out_lookup.get(normalize_for_compare(f))
            if col and col not in ordered_cols:
                ordered_cols.append(col)
        for col in gdf_sup_local.columns:
            if col == geom_name:
                continue
            if col in out_cols and col not in ordered_cols:
                ordered_cols.append(col)
        for col in out_cols.keys():
            if col == geom_name:
                continue
            if col not in ordered_cols:
                ordered_cols.append(col)
        if match_column and match_column in out_cols and match_column not in ordered_cols:
            ordered_cols.append(match_column)
        if geom_name and geom_name in out_cols:
            ordered_cols.append(geom_name)
        keep_cols = ordered_cols

    # preserve column order where possible
    out_gdf = gpd.GeoDataFrame(
        {c: out_cols[c] for c in keep_cols if c in out_cols},
        geometry=gdf_sup_local.geometry if hasattr(gdf_sup_local, "geometry") else None,
        crs=gdf_sup_local.crs,
    )

    def _is_device_target(targets: set[str]) -> bool:
        hit = normalize_for_compare(device_name) in targets
        if not hit:
            try:
                layer_norm = normalize_for_compare(layer or "")
                hit = any(target in layer_norm or layer_norm == target for target in targets)
            except Exception:
                hit = False
        if not hit:
            try:
                file_norm = normalize_for_compare(Path(file_name).stem)
                hit = any(target in file_norm or file_norm == target for target in targets)
            except Exception:
                hit = False
        return hit

    # Remove exact duplicate point features for rewritten outdoor point devices.
    if _is_device_target(
        {
            normalize_for_compare("Voltage Transformer"),
            normalize_for_compare("Current Transformer"),
            normalize_for_compare("Lightning Arrester"),
            normalize_for_compare("High Voltage Circuit Breaker/High Voltage Circuit Breaker"),
            normalize_for_compare("High Voltage Switch/High Voltage Switch"),
        }
    ) and hasattr(out_gdf, "geometry") and out_gdf.geometry is not None:
        keep_idx: list[Any] = []
        seen_geom: set[str] = set()
        for idx_val, geom in out_gdf.geometry.items():
            if geom is None or getattr(geom, "is_empty", True):
                keep_idx.append(idx_val)
                continue
            try:
                geom_key = geom.wkb_hex
            except Exception:
                try:
                    geom_key = geom.wkt
                except Exception:
                    keep_idx.append(idx_val)
                    continue
            if geom_key in seen_geom:
                continue
            seen_geom.add(geom_key)
            keep_idx.append(idx_val)
        if keep_idx and len(keep_idx) < len(out_gdf):
            out_gdf = out_gdf.loc[keep_idx].copy()

    def _fill_missing_bay_keys_by_nearest(raw_keys: list[str]) -> list[str]:
        if not raw_keys or len(raw_keys) != len(out_gdf):
            return raw_keys
        if not hasattr(out_gdf, "geometry") or out_gdf.geometry is None:
            return raw_keys
        missing_pos = [
            i for i, val in enumerate(raw_keys)
            if normalize_value_for_compare(val) == ""
        ]
        if not missing_pos:
            return raw_keys

        bay_centroid_by_key: dict[str, Any] = {}
        if isinstance(line_bay_info, dict):
            raw_centroids = line_bay_info.get("bay_centroid_by_key")
            if isinstance(raw_centroids, dict):
                for key, pt in raw_centroids.items():
                    norm_key = normalize_value_for_compare(key)
                    if not norm_key or pt is None or getattr(pt, "is_empty", True):
                        continue
                    bay_centroid_by_key[norm_key] = pt

        if not bay_centroid_by_key and isinstance(line_bay_info, dict):
            try:
                bay_gdf = load_line_bay_layer(
                    line_bay_info.get("path"),
                    line_bay_info.get("layer"),
                    line_bay_info.get("field"),
                )
            except Exception:
                bay_gdf = None
            if bay_gdf is not None and not bay_gdf.empty and hasattr(bay_gdf, "geometry"):
                bay_geom_name = bay_gdf.geometry.name
                bay_field = _pick_line_bay_name_field(bay_gdf, line_bay_info.get("field"))
                for _, row in bay_gdf.iterrows():
                    key_raw = row.get(bay_field)
                    key_norm = normalize_value_for_compare(key_raw)
                    geom = row.get(bay_geom_name)
                    if not key_norm or geom is None or getattr(geom, "is_empty", True):
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
                    bay_centroid_by_key.setdefault(key_norm, ref_pt)

        if not bay_centroid_by_key:
            return raw_keys

        resolved = list(raw_keys)
        refs = list(bay_centroid_by_key.items())
        for pos in missing_pos:
            try:
                geom = out_gdf.geometry.iloc[pos]
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
            for key_norm, ref_pt in refs:
                try:
                    dist = src_pt.distance(ref_pt)
                except Exception:
                    continue
                if best_dist is None or dist < best_dist:
                    best_dist = dist
                    best_key = key_norm
            if best_key:
                resolved[pos] = best_key
        return resolved

    def _matches_device_targets(targets: set[str]) -> bool:
        return _is_device_target(targets)

    def _order_group_indices(indices: list[Any]) -> list[Any]:
        if len(indices) <= 1 or not hasattr(out_gdf, "geometry"):
            return list(indices)
        try:
            ordered = order_indices_by_location(out_gdf.loc[indices].geometry)
            if ordered:
                return ordered
        except Exception:
            pass
        return list(indices)

    def _indices_near_busbar(max_distance: float = 2.0) -> set[Any]:
        if not hasattr(out_gdf, "geometry") or out_gdf.geometry is None:
            return set()
        if not isinstance(line_bay_info, dict):
            return set()
        busbar_geometries = line_bay_info.get("busbar_geometries")
        if not isinstance(busbar_geometries, list) or not busbar_geometries:
            return set()
        hits: set[Any] = set()
        for idx_val, geom in out_gdf.geometry.items():
            if geom is None or getattr(geom, "is_empty", True):
                continue
            try:
                pt = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
            except Exception:
                continue
            if pt is None or getattr(pt, "is_empty", True):
                continue
            for busbar_geom in busbar_geometries:
                if busbar_geom is None or getattr(busbar_geom, "is_empty", True):
                    continue
                try:
                    if pt.distance(busbar_geom) <= max_distance:
                        hits.add(idx_val)
                        break
                except Exception:
                    continue
        return hits

    def _existing_text_id_by_index(id_col: str | None) -> dict[Any, str]:
        out: dict[Any, str] = {}
        if not id_col or id_col not in out_gdf.columns:
            return out
        try:
            series = out_gdf[id_col]
        except Exception:
            return out
        for idx_val, raw_val in series.items():
            try:
                if raw_val is None or pd.isna(raw_val):
                    continue
            except Exception:
                if raw_val is None:
                    continue
            text = str(raw_val).strip()
            if text:
                out[idx_val] = text
        return out

    def _flatten_sheet_ids_by_base(sheet_ids_by_base: dict[int, list[str]]) -> list[str]:
        flat: list[str] = []
        for base in sorted(sheet_ids_by_base):
            flat.extend(sheet_ids_by_base.get(base, []))
        return flat

    def _flatten_switch_sheet_ids_by_base(sheet_ids_by_base: dict[int, dict[str, list[str]]]) -> list[str]:
        flat: list[str] = []
        def _role_sort_key(role: str) -> tuple[int, str]:
            match = re.match(r"^Q(\d+)$", str(role).strip().upper())
            if match:
                try:
                    return int(match.group(1)), role
                except Exception:
                    pass
            return 10**9, role
        for base in sorted(sheet_ids_by_base):
            role_map = sheet_ids_by_base.get(base, {})
            for role in sorted(role_map, key=_role_sort_key):
                flat.extend(role_map.get(role, []))
        return flat

    def _assign_remaining_ids(
        candidate_indices: list[Any],
        all_sheet_ids: list[str],
        used_ids: set[str],
        existing_ids_by_index: dict[Any, str],
    ) -> dict[Any, str]:
        assigned: dict[Any, str] = {}
        ordered_candidates = _order_group_indices(candidate_indices)
        remaining_lookup: dict[str, str] = {}
        for text_id in all_sheet_ids:
            norm_id = normalize_value_for_compare(text_id)
            if not norm_id or norm_id in used_ids or norm_id in remaining_lookup:
                continue
            remaining_lookup[norm_id] = text_id

        pending: list[Any] = []
        for idx_val in ordered_candidates:
            existing_id = existing_ids_by_index.get(idx_val)
            norm_existing = normalize_value_for_compare(existing_id)
            if norm_existing and norm_existing in remaining_lookup:
                assigned[idx_val] = remaining_lookup.pop(norm_existing)
                used_ids.add(norm_existing)
            else:
                pending.append(idx_val)

        remaining_ids = list(remaining_lookup.values())
        for idx_val, text_id in zip(pending, remaining_ids):
            norm_id = normalize_value_for_compare(text_id)
            if not norm_id or norm_id in used_ids:
                continue
            assigned[idx_val] = text_id
            used_ids.add(norm_id)

        return assigned

    def _parse_vt_ct_sheet_id(raw_id: Any, prefix: str) -> tuple[int, int] | None:
        if raw_id is None or pd.isna(raw_id):
            return None
        text = str(raw_id).strip().upper().replace(" ", "").replace("_", "")
        match = re.match(rf"^{prefix}0*(\d+)-?0*(\d+)$", text)
        if not match:
            return None
        try:
            return int(match.group(1)), int(match.group(2))
        except Exception:
            return None

    def _parse_cb_sheet_id(raw_id: Any) -> tuple[int, int] | None:
        if raw_id is None or pd.isna(raw_id):
            return None
        text = str(raw_id).strip().upper().replace(" ", "").replace("_", "")
        match = re.match(r"^CB0*(\d+)$", text)
        if not match:
            return None
        try:
            return int(match.group(1)), 0
        except Exception:
            return None

    def _parse_switch_sheet_id(raw_id: Any) -> tuple[int, str, int] | None:
        if raw_id is None or pd.isna(raw_id):
            return None
        text = str(raw_id).strip().upper().replace(" ", "").replace("_", "")
        match = re.match(r"^(Q\d+)-?0*(\d+)$", text)
        if not match:
            return None
        try:
            role = match.group(1)
            role_num = int(role[1:])
            return int(match.group(2)), role, role_num
        except Exception:
            return None

    def _build_sheet_ids_by_base(id_parser) -> dict[int, list[str]]:
        grouped: dict[int, list[tuple[int, str]]] = {}
        seen: dict[int, set[str]] = {}
        for entry in seq_entries:
            raw_id = entry.get("id") or entry.get("name")
            parsed = id_parser(raw_id)
            if not parsed:
                continue
            base, sort_key = parsed
            text_id = str(raw_id).strip()
            if not text_id:
                continue
            seen.setdefault(base, set())
            if text_id in seen[base]:
                continue
            seen[base].add(text_id)
            grouped.setdefault(base, []).append((sort_key, text_id))
        out: dict[int, list[str]] = {}
        for base, items in grouped.items():
            out[base] = [text_id for _, text_id in sorted(items, key=lambda t: (t[0], t[1]))]
        return out

    def _build_switch_sheet_ids_by_base() -> dict[int, dict[str, list[str]]]:
        grouped: dict[int, dict[str, list[str]]] = {}
        seen: dict[int, dict[str, set[str]]] = {}
        for entry in seq_entries:
            raw_id = entry.get("id") or entry.get("name")
            parsed = _parse_switch_sheet_id(raw_id)
            if not parsed:
                continue
            base, role, sort_key = parsed
            text_id = str(raw_id).strip()
            if not text_id:
                continue
            seen.setdefault(base, {}).setdefault(role, set())
            if text_id in seen[base][role]:
                continue
            seen[base][role].add(text_id)
            grouped.setdefault(base, {}).setdefault(role, []).append((sort_key, text_id))
        out: dict[int, dict[str, list[str]]] = {}
        for base, role_map in grouped.items():
            out[base] = {}
            for role, items in role_map.items():
                out[base][role] = [text_id for _, text_id in sorted(items, key=lambda t: (t[0], t[1]))]
        return out

    def _resolve_bay_keys_and_base() -> tuple[list[str], dict[str, int]]:
        if len(out_gdf) <= 0:
            return [], {}
        norm_lookup = {normalize_for_compare(c): c for c in out_gdf.columns}
        line_bay_aliases = [
            "Line_Bay_ID",
            "Line Bay ID",
            "LineBayID",
            "LineBay_ID",
            "Line_Bay_Name",
            "Line Bay Name",
            "LineBayName",
            "Line Bay",
            "Line_Bay",
        ]
        line_bay_col = None
        for alias in line_bay_aliases:
            col = norm_lookup.get(normalize_for_compare(alias))
            if col:
                line_bay_col = col
                break
        bay_keys_raw: list[str] = []
        if (
            line_bay_info is not None
            and hasattr(out_gdf, "geometry")
            and out_gdf.geometry is not None
        ):
            try:
                spatial_series = build_spatial_match_targets(
                    out_gdf,
                    line_bay_info.get("path"),
                    line_bay_info.get("layer"),
                    line_bay_info.get("field"),
                    allow_nearest_fallback=False,
                )
                if spatial_series is not None and len(spatial_series) == len(out_gdf):
                    vals = [normalize_value_for_compare(v) for v in spatial_series.tolist()]
                    if any(vals):
                        bay_keys_raw = vals
            except Exception:
                pass
        if not bay_keys_raw and line_bay_col:
            bay_keys_raw = [normalize_value_for_compare(v) for v in out_gdf[line_bay_col].tolist()]
        if bay_keys_raw:
            has_any_key = any(normalize_value_for_compare(v) for v in bay_keys_raw)
            if not has_any_key:
                bay_keys_raw = []
        if not bay_keys_raw:
            return [], {}

        id_name_map = line_bay_info.get("id_name_map") if isinstance(line_bay_info, dict) else {}
        reverse_name_to_id: dict[str, str] = {}
        if isinstance(id_name_map, dict):
            for k, v in id_name_map.items():
                k_norm = normalize_value_for_compare(k)
                v_norm = normalize_value_for_compare(v)
                if k_norm and v_norm and v_norm not in reverse_name_to_id:
                    reverse_name_to_id[v_norm] = k_norm

        def _canon(raw_val: Any) -> str:
            norm_val = normalize_value_for_compare(raw_val)
            if not norm_val:
                return ""
            mapped = reverse_name_to_id.get(norm_val, norm_val)
            try:
                match = re.search(r"e0*(\d+)", mapped)
                if match:
                    return f"e{int(match.group(1))}"
            except Exception:
                pass
            return mapped

        def _extract_base(raw_val: Any) -> int | None:
            norm_val = _canon(raw_val)
            if not norm_val:
                return None
            try:
                m = re.search(r"e0*(\d+)", norm_val)
                if m:
                    return int(m.group(1))
                nums = re.findall(r"\d+", norm_val)
                if nums:
                    return int(nums[-1])
            except Exception:
                return None
            return None

        bay_keys = [_canon(v) for v in bay_keys_raw]
        unique = []
        seen = set()
        for k in bay_keys:
            if k not in seen:
                seen.add(k)
                unique.append(k)
        bay_base: dict[str, int] = {}
        used: set[int] = set()
        for k in unique:
            if not k:
                continue
            b = _extract_base(k)
            if b is not None and b > 0:
                bay_base[k] = b
                used.add(b)
        nxt = max(used) + 1 if used else 1
        for k in unique:
            if not k:
                continue
            if k in bay_base:
                continue
            while nxt in used:
                nxt += 1
            bay_base[k] = nxt
            used.add(nxt)
            nxt += 1
        return bay_keys, bay_base

    def _assign_sheet_device_ids_from_line_bay(
        device_targets: set[str],
        id_aliases: list[str],
        name_aliases: list[str],
        default_id_col: str,
        id_prefix: str,
    ) -> None:
        nonlocal out_gdf
        if len(out_gdf) <= 0 or not _matches_device_targets(device_targets):
            return

        bay_keys, bay_base = _resolve_bay_keys_and_base()
        if not bay_keys or len(bay_keys) != len(out_gdf):
            return

        norm_lookup = {normalize_for_compare(c): c for c in out_gdf.columns}
        id_col = None
        for alias in id_aliases:
            col = norm_lookup.get(normalize_for_compare(alias))
            if col:
                id_col = col
                break
        if id_col is None:
            id_col = default_id_col
            out_gdf[id_col] = pd.NA
        existing_ids_by_index = _existing_text_id_by_index(id_col)

        name_cols: list[str] = []
        for alias in name_aliases:
            col = norm_lookup.get(normalize_for_compare(alias))
            if col and col not in name_cols:
                name_cols.append(col)

        sheet_ids_by_base = _build_sheet_ids_by_base(
            lambda raw_id: _parse_vt_ct_sheet_id(raw_id, id_prefix)
        )
        has_sheet_ids = bool(sheet_ids_by_base)
        if not has_sheet_ids:
            return
        exclude_from_bay_matching: set[Any] = set()
        if id_prefix == "VT":
            exclude_from_bay_matching = _indices_near_busbar()

        groups: dict[str, list[Any]] = {}
        for idx_val, key in zip(list(out_gdf.index), bay_keys):
            if idx_val in exclude_from_bay_matching:
                key = ""
            groups.setdefault(key, []).append(idx_val)

        selected: list[Any] = []
        new_ids: dict[Any, str] = {}
        used_sheet_ids: set[str] = set()
        for key, idxs in groups.items():
            base = bay_base.get(key)
            if base is None:
                continue
            ids_for_base = sheet_ids_by_base.get(base, [])
            if has_sheet_ids and not ids_for_base:
                continue
            ordered = _order_group_indices(idxs)
            if ids_for_base:
                for idx_val, sheet_id in zip(ordered, ids_for_base):
                    selected.append(idx_val)
                    new_ids[idx_val] = sheet_id
                    norm_id = normalize_value_for_compare(sheet_id)
                    if norm_id:
                        used_sheet_ids.add(norm_id)

        if new_ids:
            remaining_ids = _assign_remaining_ids(
                [idx_val for idx_val in out_gdf.index if idx_val not in new_ids],
                _flatten_sheet_ids_by_base(sheet_ids_by_base),
                used_sheet_ids,
                existing_ids_by_index,
            )
            new_ids.update(remaining_ids)
            selected_set = set(new_ids)
            ordered_selected = [idx_val for idx_val in out_gdf.index if idx_val in selected_set]
            out_gdf = out_gdf.loc[ordered_selected].copy()
            for idx_val, sheet_id in new_ids.items():
                if idx_val not in out_gdf.index:
                    continue
                out_gdf.at[idx_val, id_col] = sheet_id
                for col in name_cols:
                    out_gdf.at[idx_val, col] = sheet_id
            return

    # Post-fill: assign workbook VT IDs to bays using the Line Bay arrangement logic.
    _assign_sheet_device_ids_from_line_bay(
        {
            normalize_for_compare("Voltage Transformer"),
        },
        [
            "VoltageTransfomer_ID",
            "Voltage Transformer ID",
            "VoltageTransfomerID",
            "Voltage Transformer Id",
            "VoltageTransformerID",
        ],
        [
            "Voltage Transformer Name",
            "VoltageTransfomer_Name",
            "VoltageTransformerName",
            "Name",
            "name",
        ],
        default_id_col="VoltageTransfomer_ID",
        id_prefix="VT",
    )

    # Post-fill: assign workbook CT IDs to bays using the Line Bay arrangement logic.
    _assign_sheet_device_ids_from_line_bay(
        {
            normalize_for_compare("Current Transformer"),
        },
        [
            "CurrentTransfomer_ID",
            "CurrentTransformer_ID",
            "Current Transformer ID",
            "Current Transformer Id",
            "CurrentTransfomerID",
            "CurrentTransformerID",
            "Current Transfomer ID",
        ],
        [
            "Current Transformer Name",
            "CurrentTransfomer_Name",
            "CurrentTransformerName",
            "Name",
            "name",
        ],
        default_id_col="CurrentTransfomerID",
        id_prefix="CT",
    )

    # Post-fill: assign workbook Lightning Arrester IDs using line-bay arrangement where applicable.
    _assign_sheet_device_ids_from_line_bay(
        {
            normalize_for_compare("Lightning Arrester"),
        },
        [
            "ArresterID",
            "Arrester_ID",
            "Arrester ID",
            "LightningArresterID",
            "Lightning Arrester ID",
        ],
        [
            "Lightining Arrester Name",
            "Lightning Arrester Name",
            "Arrester Name",
            "Name",
            "name",
        ],
        default_id_col="ArresterID",
        id_prefix="SA",
    )

    # Post-fill: keep one middle HV CB per bay using workbook IDs only.
    if _matches_device_targets({normalize_for_compare("High Voltage Circuit Breaker/High Voltage Circuit Breaker")}):
        bay_keys, bay_base = _resolve_bay_keys_and_base()
        if bay_keys and len(bay_keys) == len(out_gdf):
            norm_lookup = {normalize_for_compare(c): c for c in out_gdf.columns}
            cb_id_aliases = ["CircuitBreakerID", "CircuitBreaker_ID", "Circuit Breaker ID", "Circuit BreakerID"]
            cb_name_aliases = ["Circuit Breaker Name", "CircuitBreakerName", "Name", "name"]
            cb_id_col = None
            for alias in cb_id_aliases:
                col = norm_lookup.get(normalize_for_compare(alias))
                if col:
                    cb_id_col = col
                    break
            if cb_id_col is None:
                cb_id_col = "CircuitBreakerID"
                out_gdf[cb_id_col] = pd.NA
            existing_cb_ids_by_index = _existing_text_id_by_index(cb_id_col)

            cb_sheet_ids_by_base = _build_sheet_ids_by_base(_parse_cb_sheet_id)
            has_cb_sheet_ids = bool(cb_sheet_ids_by_base)
            bay_centroids = line_bay_info.get("bay_centroid_by_key", {}) if isinstance(line_bay_info, dict) else {}

            def _point_dist(idx_val: Any, ref_pt: Any) -> float:
                if ref_pt is None:
                    return float("inf")
                if not hasattr(out_gdf, "geometry"):
                    return float("inf")
                try:
                    geom = out_gdf.loc[idx_val, out_gdf.geometry.name]
                except Exception:
                    return float("inf")
                if geom is None or getattr(geom, "is_empty", True):
                    return float("inf")
                try:
                    pt = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
                    return float(pt.distance(ref_pt))
                except Exception:
                    return float("inf")

            groups: dict[str, list[Any]] = {}
            for idx_val, key in zip(list(out_gdf.index), bay_keys):
                groups.setdefault(key, []).append(idx_val)

            selected: list[Any] = []
            cb_new_ids: dict[Any, str] = {}
            used_cb_ids: set[str] = set()
            for key, idxs in groups.items():
                if not idxs:
                    continue
                if not key:
                    continue
                base = bay_base.get(key)
                if base is None:
                    continue
                chosen = None
                ref_center = bay_centroids.get(key) if isinstance(bay_centroids, dict) else None
                if ref_center is not None:
                    try:
                        chosen = min(idxs, key=lambda i: _point_dist(i, ref_center))
                    except Exception:
                        chosen = None
                if chosen is None:
                    try:
                        ordered = order_indices_by_location(out_gdf.loc[idxs].geometry)
                        if ordered:
                            chosen = ordered[len(ordered) // 2]
                    except Exception:
                        chosen = None
                if chosen is None:
                    chosen = idxs[len(idxs) // 2]
                sheet_ids = cb_sheet_ids_by_base.get(base, [])
                if not sheet_ids:
                    continue
                selected.append(chosen)
                cb_id = sheet_ids[0]
                cb_new_ids[chosen] = cb_id
                norm_id = normalize_value_for_compare(cb_id)
                if norm_id:
                    used_cb_ids.add(norm_id)

            remaining_cb_ids = _assign_remaining_ids(
                [idx_val for idx_val in out_gdf.index if idx_val not in cb_new_ids],
                _flatten_sheet_ids_by_base(cb_sheet_ids_by_base),
                used_cb_ids,
                existing_cb_ids_by_index,
            )
            cb_new_ids.update(remaining_cb_ids)

            selected_set = set(cb_new_ids)
            ordered_selected = [idx_val for idx_val in out_gdf.index if idx_val in selected_set]
            if ordered_selected:
                out_gdf = out_gdf.loc[ordered_selected].copy()
                cb_name_cols: list[str] = []
                for alias in cb_name_aliases:
                    col = norm_lookup.get(normalize_for_compare(alias))
                    if col and col not in cb_name_cols:
                        cb_name_cols.append(col)
                for idx_val, newid in cb_new_ids.items():
                    if idx_val not in out_gdf.index:
                        continue
                    out_gdf.at[idx_val, cb_id_col] = newid
                    for col in cb_name_cols:
                        out_gdf.at[idx_val, col] = newid

    # Post-fill: assign disconnector IDs from workbook rows, keeping Q9 on the line-exit side.
    if _matches_device_targets({normalize_for_compare("High Voltage Switch/High Voltage Switch")}):
        bay_keys, bay_base = _resolve_bay_keys_and_base()
        if bay_keys and len(bay_keys) == len(out_gdf):
            norm_lookup = {normalize_for_compare(c): c for c in out_gdf.columns}
            sw_id_aliases = [
                "HV_Switch_ID",
                "HV Switch ID",
                "HVSwitchID",
                "Disconnector_ID",
                "Disconnector ID",
            ]
            sw_name_aliases = ["HV Switch Name", "HVSwitchName", "Disconnector Switch Name", "Name", "name"]
            sw_id_col = None
            for alias in sw_id_aliases:
                col = norm_lookup.get(normalize_for_compare(alias))
                if col:
                    sw_id_col = col
                    break
            if sw_id_col is None:
                sw_id_col = "HV_Switch_ID"
                out_gdf[sw_id_col] = pd.NA
            existing_sw_ids_by_index = _existing_text_id_by_index(sw_id_col)

            sw_sheet_ids_by_base = _build_switch_sheet_ids_by_base()
            has_sw_sheet_ids = bool(sw_sheet_ids_by_base)
            bay_centroids = line_bay_info.get("bay_centroid_by_key", {}) if isinstance(line_bay_info, dict) else {}
            line_exit_refs = line_bay_info.get("line_exit_ref_by_key", {}) if isinstance(line_bay_info, dict) else {}
            vt_refs = line_bay_info.get("vt_ref_by_key", {}) if isinstance(line_bay_info, dict) else {}

            def _point_dist(idx_val: Any, ref_pt: Any) -> float:
                if ref_pt is None:
                    return float("inf")
                if not hasattr(out_gdf, "geometry"):
                    return float("inf")
                try:
                    geom = out_gdf.loc[idx_val, out_gdf.geometry.name]
                except Exception:
                    return float("inf")
                if geom is None or getattr(geom, "is_empty", True):
                    return float("inf")
                try:
                    pt = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
                    return float(pt.distance(ref_pt))
                except Exception:
                    return float("inf")

            groups: dict[str, list[Any]] = {}
            for idx_val, key in zip(list(out_gdf.index), bay_keys):
                groups.setdefault(key, []).append(idx_val)

            selected: list[Any] = []
            sw_new_ids: dict[Any, str] = {}
            used_sw_ids: set[str] = set()
            for key, idxs in groups.items():
                if not idxs:
                    continue
                if not key:
                    continue
                base = bay_base.get(key)
                if base is None:
                    continue

                ref_exit = line_exit_refs.get(key) if isinstance(line_exit_refs, dict) else None
                ref_vt = vt_refs.get(key) if isinstance(vt_refs, dict) else None
                ref_center = bay_centroids.get(key) if isinstance(bay_centroids, dict) else None
                sheet_ids = sw_sheet_ids_by_base.get(base, {})
                q9_ids = sheet_ids.get("Q9", [])
                q1_ids = sheet_ids.get("Q1", [])
                if not sheet_ids:
                    continue

                if ref_exit is not None:
                    q9_idx = min(idxs, key=lambda i: _point_dist(i, ref_exit))
                elif ref_vt is not None:
                    q9_idx = max(idxs, key=lambda i: _point_dist(i, ref_vt))
                elif ref_center is not None:
                    q9_idx = max(idxs, key=lambda i: _point_dist(i, ref_center))
                else:
                    try:
                        ordered = order_indices_by_location(out_gdf.loc[idxs].geometry)
                        q9_idx = ordered[0] if ordered else idxs[0]
                    except Exception:
                        q9_idx = idxs[0]

                if q9_ids:
                    q9_id = q9_ids[0]
                    selected.append(q9_idx)
                    sw_new_ids[q9_idx] = q9_id
                    norm_id = normalize_value_for_compare(q9_id)
                    if norm_id:
                        used_sw_ids.add(norm_id)

                remaining = [i for i in idxs if i != q9_idx]
                if remaining:
                    if ref_exit is not None:
                        q1_idx = max(remaining, key=lambda i: _point_dist(i, ref_exit))
                    elif ref_vt is not None:
                        q1_idx = min(remaining, key=lambda i: _point_dist(i, ref_vt))
                    elif ref_center is not None:
                        q1_idx = min(remaining, key=lambda i: _point_dist(i, ref_center))
                    else:
                        q1_idx = remaining[-1]
                    if q1_ids:
                        q1_id = q1_ids[0]
                        selected.append(q1_idx)
                        sw_new_ids[q1_idx] = q1_id
                        norm_id = normalize_value_for_compare(q1_id)
                        if norm_id:
                            used_sw_ids.add(norm_id)

            remaining_sw_ids = _assign_remaining_ids(
                [idx_val for idx_val in out_gdf.index if idx_val not in sw_new_ids],
                _flatten_switch_sheet_ids_by_base(sw_sheet_ids_by_base),
                used_sw_ids,
                existing_sw_ids_by_index,
            )
            sw_new_ids.update(remaining_sw_ids)

            selected_set = set(sw_new_ids)
            ordered_selected = [idx_val for idx_val in out_gdf.index if idx_val in selected_set]
            if ordered_selected:
                out_gdf = out_gdf.loc[ordered_selected].copy()
                sw_name_cols: list[str] = []
                for alias in sw_name_aliases:
                    col = norm_lookup.get(normalize_for_compare(alias))
                    if col and col not in sw_name_cols:
                        sw_name_cols.append(col)
                for idx_val, newid in sw_new_ids.items():
                    if idx_val not in out_gdf.index:
                        continue
                    out_gdf.at[idx_val, sw_id_col] = newid
                    for col in sw_name_cols:
                        out_gdf.at[idx_val, col] = newid

    # Post-fill: align High Voltage Line names to intersecting/nearest Line Bay (uploaded HV lines).
    hv_name_norm = normalize_for_compare("High Voltage Line")
    hv_match = normalize_for_compare(device_name) == hv_name_norm
    if not hv_match:
        try:
            layer_norm = normalize_for_compare(layer or "")
            hv_match = hv_name_norm in layer_norm or layer_norm == hv_name_norm
        except Exception:
            hv_match = False
    if not hv_match:
        try:
            file_norm = normalize_for_compare(Path(file_name).stem)
            hv_match = hv_name_norm in file_norm or file_norm == hv_name_norm
        except Exception:
            hv_match = False
    if hv_match and geom_name and hasattr(out_gdf, "geometry"):
        # Fall back to the Line Bay library folder if no Line Bay info was provided.
        lb_info = line_bay_info
        if lb_info is None and LINE_BAY_LIBRARY_PATH.exists():
            lb_info = {
                "path": LINE_BAY_LIBRARY_PATH,
                "layer": None,
                "field": None,
                "id_name_map": {},
            }
        if lb_info:
            out_gdf = apply_line_bay_names(out_gdf, lb_info, geom_name)
            id_name_map = lb_info.get("id_name_map") if isinstance(lb_info, dict) else {}
        else:
            id_name_map = {}
        if isinstance(id_name_map, dict) and id_name_map:
            out_gdf = replace_line_name_ids(out_gdf, id_name_map)

    out_gdf = sanitize_gdf_for_gpkg(out_gdf)
    # Re-apply declared types after sanitization to preserve int widths (e.g., Short Integer).
    if type_map_local:
        norm_type_lookup = {
            normalize_for_compare(k): v for k, v in type_map_local.items() if v is not None
        }
        geom_name_out = out_gdf.geometry.name if hasattr(out_gdf, "geometry") else None
        for col_name in out_gdf.columns:
            if col_name == geom_name_out:
                continue
            norm_col = normalize_for_compare(col_name)
            t_str = preserve_type_map.get(norm_col)
            if t_str is None:
                t_str = type_map_local.get(col_name)
            if t_str is None:
                t_str = norm_type_lookup.get(norm_col)
            if t_str:
                try:
                    out_gdf[col_name] = coerce_series_to_type(out_gdf[col_name], t_str)
                except Exception:
                    pass
    if normalize_for_compare(device_name) in ASPATIAL_DEVICES:
        geom_name_out = out_gdf.geometry.name if hasattr(out_gdf, "geometry") else None
        out_df = pd.DataFrame(out_gdf.drop(columns=[geom_name_out], errors="ignore"))
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
            out_path = Path(tmpout.name)
        write_aspatial_gpkg_layer(out_df, out_path, layer)
        return out_path, layer
    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmpout:
        out_path = Path(tmpout.name)
    out_gdf.to_file(out_path, driver="GPKG", layer=layer)
    return out_path, layer


FILL_BATCH_DEPS = FillBatchDependencies(
    fill_one_gpkg=fill_one_gpkg,
    collect_device_polygons_from_uploads=collect_device_polygons_from_uploads,
    build_cabin_anchor_points=build_cabin_anchor_points,
    build_control_panel_polygons=build_control_panel_polygons,
    enrich_line_bay_reference_info=enrich_line_bay_reference_info,
    load_ups_anchor_and_crs=load_ups_anchor_and_crs,
    build_points_in_panel_polygons=build_points_in_panel_polygons,
    build_protection_layout_points=build_protection_layout_points,
    layout_points_in_cabins=layout_points_in_cabins,
    expand_geometries=expand_geometries,
    load_template_layer=load_template_layer,
    load_line_bay_layer=load_line_bay_layer,
    pick_line_bay_name_field=_pick_line_bay_name_field,
    extract_bay_name_from_row=_extract_bay_name_from_row,
    collect_device_points_from_uploads=collect_device_points_from_uploads,
    collect_point_geometries_from_uploads=collect_point_geometries_from_uploads,
    map_points_to_bays=map_points_to_bays,
    build_lines_from_points_in_polygon=build_lines_from_points_in_polygon,
    replace_line_name_ids=replace_line_name_ids,
    ensure_name_fields_string=ensure_name_fields_string,
)

