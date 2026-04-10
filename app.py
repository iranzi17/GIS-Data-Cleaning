import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
import difflib
import re
import io

import geopandas as gpd
import pandas as pd
import streamlit as st

from gis_loader.config import (
    EARTHING_TRANSFORMER_TEMPLATE_PATH,
    ELECTRIC_DEVICE_EQUIPMENT,
    HV_LINE_TEMPLATE_PATH,
    LINE_BAY_LIBRARY_PATH,
    PREVIEW_ROWS,
    SUPERVISOR_WORKBOOK_DIRS,
    WORKBOOK_PRIORITY,
)
from gis_loader.data_sources import (
    build_supervisor_workbook_index as _build_supervisor_workbook_index,
    coerce_gpkg_path as _coerce_gpkg_path,
    get_excel_file,
    get_file_name as _get_file_name,
    list_gpkg_layers,
    list_reference_workbooks,
    list_supervisor_workbooks,
    load_gpkg_equipment_map,
    load_mapping_cache,
    pick_supervisor_sheet,
    resolve_supervisor_workbook_for_substation,
    save_mapping_cache,
)
from gis_loader.batch import process_single_gpkg
from gis_loader.equipment import (
    PROTECTION_LAYOUT_DEVICES,
    fuzzy_map_columns_with_scores,
    resolve_equipment_name,
)
from gis_loader.fill import (
    FORCED_CABIN_AUTO_CREATE_DEVICES,
    LINE_BAY_SPATIAL_DEVICES,
    PROTECTION_LAYOUT_SPACING,
    SEQUENTIAL_FILL_DEVICES,
    SKIP_BATCH_FILL_STEMS,
    append_domain_code_log,
    build_device_gdf_from_instances,
    build_device_table_from_instances,
    collect_domain_log_entries as _collect_domain_log_entries,
    domain_log_rows_to_csv,
    ensure_name_fields_string,
    fill_supervisor_batch,
    repeat_instances,
)
from gis_loader.fill_engine import FILL_BATCH_DEPS, fill_one_gpkg
from gis_loader.matching import (
    detect_join_columns,
    match_overrides_for_file,
    preferred_match_columns,
    select_sheet_for_gpkg,
)
from gis_loader.schema import (
    apply_global_forward_fill as _apply_global_forward_fill,
    clean_empty_rows,
    coerce_series_to_type,
    detect_header_row as _detect_header_row,
    list_schema_equipments,
    load_reference_sheet,
    load_schema_fields,
)
from gis_loader.spatial import (
    _build_line_bay_id_name_map,
    _extract_bay_name_from_row,
    _pick_line_bay_name_field,
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
    layout_points_in_cabins,
    load_line_bay_layer,
    load_template_layer,
    load_ups_anchor_and_crs,
    map_points_to_bays,
    replace_line_name_ids,
)
from gis_loader.supervisor import parse_supervisor_device_table
from gis_loader.ui_support import (
    detect_substation_column,
    forward_fill_column,
    merge_without_duplicates,
    st_dataframe_safe,
)
from gis_loader.geopackage import (
    derive_layer_name_from_filename,
    sanitize_gdf_for_gpkg,
    write_aspatial_gpkg_layer,
)
from gis_loader.text import (
    INVISIBLE_HEADER_CHARS,
    clean_column_name as _clean_column_name,
    ensure_unique_columns,
    normalize_for_compare,
    normalize_value_for_compare,
)

_SUB_COL_CACHE: dict[tuple[str, str], str | None] = {}


def run_app() -> None:
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
    seq_assign_fallback = True
    sup_gpkg_zip = st.file_uploader(
        "Upload ZIP(s) of substation folders (each folder name = substation)",
        type=["zip"],
        accept_multiple_files=True,
        key="sup_gpkg_zip",
    )
    if sup_gpkg_zip:
        st.caption(
            "Example ZIP structure: NDERA/LINE BAY.gpkg, NDERA/High Voltage Line.gpkg, BUGARAMA/LINE BAY.gpkg"
        )
    sup_gpkg_files = st.file_uploader(
        "Upload device GeoPackage (GPKG)", type=["gpkg"], accept_multiple_files=True, key="sup_gpkg"
    )
    sup_wb_files = list_supervisor_workbooks()
    sup_wb_path = None
    if sup_wb_files:
        folders_label = ", ".join(str(p) for p in SUPERVISOR_WORKBOOK_DIRS)
        st.caption(f"Supervisor workbooks folders (New Data preferred): {folders_label}")
        sup_wb_label = st.selectbox("Supervisor workbook (Electric device format)", list(sup_wb_files.keys()), key="sup_wb_select")
        sup_wb_path = sup_wb_files[sup_wb_label]
    else:
        folders_label = ", ".join(str(p) for p in SUPERVISOR_WORKBOOK_DIRS)
        st.info(f"Add supervisor workbooks to: {folders_label}")

    if sup_gpkg_zip:
        if st.button("Fill all substation folders (zip)", key="sup_fill_zip"):
            wb_map = list_supervisor_workbooks()
            if not wb_map:
                folders_label = ", ".join(str(p) for p in SUPERVISOR_WORKBOOK_DIRS)
                st.error(f"No supervisor workbooks found in {folders_label}.")
            else:
                tmp_in_dir = Path(tempfile.mkdtemp())
                logs: list[str] = []
                outputs: list[tuple[str, Path]] = []
                run_domain_rows: list[dict[str, Any]] = []
                run_id_validation_rows: list[dict[str, Any]] = []
                try:
                    zip_files = sup_gpkg_zip if isinstance(sup_gpkg_zip, list) else [sup_gpkg_zip]
                    gpkg_paths: list[Path] = []
                    zip_root_labels: dict[str, str] = {}
                    for idx, zip_file in enumerate(zip_files, start=1):
                        zip_path = tmp_in_dir / f"sup_batch_{idx}.zip"
                        with open(zip_path, "wb") as f:
                            f.write(zip_file.getbuffer())
                        extract_dir = tmp_in_dir / f"zip_{idx}"
                        extract_dir.mkdir(parents=True, exist_ok=True)
                        zip_root_labels[extract_dir.name] = Path(getattr(zip_file, "name", f"zip_{idx}")).stem
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            zf.extractall(extract_dir)
                        gpkg_paths.extend(list(extract_dir.rglob("*.gpkg")))

                    if not gpkg_paths:
                        st.error("No GeoPackages found inside the ZIP(s).")
                    else:
                        wb_index = _build_supervisor_workbook_index(wb_map)
                        equip_map_sup = load_gpkg_equipment_map()

                        groups: dict[str, list[Path]] = {}
                        for gpkg_path in gpkg_paths:
                            rel_parts = gpkg_path.relative_to(tmp_in_dir).parts
                            zip_root = rel_parts[0] if rel_parts else ""
                            if rel_parts and rel_parts[0].startswith("zip_"):
                                rel_parts = rel_parts[1:]
                            if len(rel_parts) > 1:
                                substation_name = rel_parts[0]
                            else:
                                substation_name = zip_root_labels.get(zip_root, gpkg_path.stem)
                            groups.setdefault(substation_name, []).append(gpkg_path)

                        for substation_name, files in sorted(groups.items(), key=lambda x: x[0]):
                            wb_label, wb_path = resolve_supervisor_workbook_for_substation(
                                substation_name, wb_map, wb_index
                            )
                            if wb_path is None:
                                logs.append(f"{substation_name}: skipped (no matching supervisor workbook).")
                                continue
                            try:
                                sup_excel = pd.ExcelFile(wb_path)
                            except Exception as exc:
                                logs.append(f"{substation_name}: failed to read workbook '{wb_label}' ({exc}).")
                                continue
                            sup_sheet = pick_supervisor_sheet(sup_excel)
                            if not sup_sheet:
                                logs.append(f"{substation_name}: skipped (no sheets found in '{wb_label}').")
                                continue
                            try:
                                raw_sup = pd.read_excel(wb_path, sheet_name=sup_sheet, dtype=str, header=None)
                                if raw_sup.empty:
                                    logs.append(f"{substation_name}: skipped (sheet '{sup_sheet}' is empty).")
                                    continue
                                raw_sup.iloc[:, 0] = raw_sup.iloc[:, 0].ffill()
                                device_options = sorted(set(raw_sup.iloc[:, 0].dropna().astype(str)))
                            except Exception as exc:
                                logs.append(f"{substation_name}: failed to read '{sup_sheet}' in '{wb_label}' ({exc}).")
                                continue
                            if not device_options:
                                logs.append(f"{substation_name}: skipped (no devices found in '{wb_label}').")
                                continue

                            protection_in_uploads = False
                            ups_upload_candidate = None
                            line_bay_upload_candidate = None
                            for file_path in files:
                                try:
                                    dev_name = resolve_equipment_name(file_path.name, device_options, equip_map_sup)
                                except Exception:
                                    continue
                                if normalize_for_compare(dev_name) in PROTECTION_LAYOUT_DEVICES:
                                    protection_in_uploads = True
                                if (
                                    ups_upload_candidate is None
                                    and normalize_for_compare(dev_name) == normalize_for_compare("Uninterruptable power supply(UPS)")
                                ):
                                    ups_upload_candidate = file_path
                                if (
                                    line_bay_upload_candidate is None
                                    and normalize_for_compare(dev_name) == normalize_for_compare("Line Bay")
                                ):
                                    line_bay_upload_candidate = file_path
                            if ups_upload_candidate is None:
                                for file_path in files:
                                    if "ups" in normalize_for_compare(Path(file_path.name).stem):
                                        ups_upload_candidate = file_path
                                        break
                            if line_bay_upload_candidate is None:
                                for file_path in files:
                                    stem_norm = normalize_for_compare(Path(file_path.name).stem)
                                    if "linebay" in stem_norm or "line bay" in stem_norm or "line_bay" in stem_norm:
                                        line_bay_upload_candidate = file_path
                                        break

                            line_bay_info = None
                            if line_bay_upload_candidate is not None:
                                try:
                                    line_bay_layers = list_gpkg_layers(line_bay_upload_candidate)
                                    if line_bay_layers:
                                        line_bay_layer = line_bay_layers[0]
                                        gdf_bay_preview = gpd.read_file(line_bay_upload_candidate, layer=line_bay_layer)
                                        geom_col = (
                                            gdf_bay_preview.geometry.name
                                            if hasattr(gdf_bay_preview, "geometry")
                                            else None
                                        )
                                        candidate_cols = [c for c in gdf_bay_preview.columns if c != geom_col]
                                        if candidate_cols:
                                            def _score_bay_col(col: str) -> int:
                                                norm = normalize_for_compare(col)
                                                score = 0
                                                if "name" in norm:
                                                    score += 3
                                                if "bay" in norm:
                                                    score += 2
                                                if "line" in norm:
                                                    score += 1
                                                if "id" in norm:
                                                    score -= 2
                                                return score
                                            default_col = sorted(candidate_cols, key=lambda c: (-_score_bay_col(c), len(c)))[0]
                                            line_bay_info = {
                                                "path": line_bay_upload_candidate,
                                                "layer": line_bay_layer,
                                                "field": default_col,
                                                "id_name_map": _build_line_bay_id_name_map(wb_path, sup_sheet),
                                            }
                                except Exception:
                                    line_bay_info = None

                            ups_anchor_info = None
                            if ups_upload_candidate is not None:
                                try:
                                    ups_layers = list_gpkg_layers(ups_upload_candidate)
                                    if ups_layers:
                                        ups_anchor_info = {
                                            "path": ups_upload_candidate,
                                            "layer": ups_layers[0],
                                            "spacing": float(PROTECTION_LAYOUT_SPACING),
                                        }
                                except Exception:
                                    ups_anchor_info = None
                            elif protection_in_uploads:
                                logs.append(f"{substation_name}: protection layout skipped (UPS not found).")

                            logs.append(f"{substation_name}: using workbook '{wb_label}' (sheet '{sup_sheet}').")

                            try:
                                batch_outputs, batch_logs, batch_domain_rows = fill_supervisor_batch(
                                    files,
                                    device_options,
                                    wb_path,
                                    sup_sheet,
                                    equip_map_sup,
                                    line_bay_info,
                                    ups_anchor_info,
                                    FILL_BATCH_DEPS,
                                    seq_assign_fallback=seq_assign_fallback,
                                    output_prefix=substation_name,
                                    id_validation_rows=run_id_validation_rows,
                                )
                                outputs.extend(batch_outputs)
                                logs.extend(batch_logs)
                                run_domain_rows.extend(batch_domain_rows)
                            except Exception as exc:
                                logs.append(f"{substation_name}: batch failed ({type(exc).__name__}: {exc})")
                                try:
                                    import traceback

                                    logs.append(traceback.format_exc())
                                except Exception:
                                    pass

                    if outputs:
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as ztmp:
                            zip_path = Path(ztmp.name)
                        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            for name, out_path in outputs:
                                zf.write(out_path, arcname=name)
                            if run_domain_rows:
                                zf.writestr("domain_code_log.csv", domain_log_rows_to_csv(run_domain_rows))
                            if run_id_validation_rows:
                                zf.writestr(
                                    "rewritten_id_validation_report.csv",
                                    id_validation_rows_to_csv(run_id_validation_rows),
                                )
                        with open(zip_path, "rb") as f:
                            data = f.read()
                        st.download_button(
                            "Download filled GeoPackages (zip)",
                            data=data,
                            file_name="filled_supervisor_gpkgs.zip",
                            mime="application/zip",
                            key="sup_download_zip_batch",
                        )
                    st.text_area("Supervisor batch log", value="\n".join(logs) if logs else "No logs.", height=220)
                finally:
                    shutil.rmtree(tmp_in_dir, ignore_errors=True)
    elif sup_gpkg_files and sup_wb_path:
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
                or LINE_BAY_LIBRARY_PATH.exists()
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
                elif LINE_BAY_LIBRARY_PATH.exists():
                    line_bay_path = LINE_BAY_LIBRARY_PATH
                    line_bay_label = LINE_BAY_LIBRARY_PATH.name
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
                                    # Strongly prefer name-bearing columns; de-prioritize ids.
                                    if "name" in norm:
                                        score += 3
                                    if "bay" in norm:
                                        score += 2
                                    if "line" in norm:
                                        score += 1
                                    if "id" in norm:
                                        score -= 2
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
                                        "id_name_map": _build_line_bay_id_name_map(sup_wb_path, sup_sheet),
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
            device_type_map = device_instances[0].get("type_map", {}) if device_instances else {}
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

                        is_line_bay = normalize_for_compare(device_choice) == normalize_for_compare("Line Bay")

                        def _score_col(col: str) -> int:
                            norm = normalize_for_compare(col)
                            score = 0
                            # Prefer names for Line Bay to avoid defaulting to IDs; otherwise balanced keyword scoring.
                            if "name" in norm:
                                score += 3 if is_line_bay else 1
                            if "bay" in norm:
                                score += 2 if is_line_bay else 1
                            if "line" in norm:
                                score += 1
                            if "id" in norm:
                                score += -2 if is_line_bay else 1
                            for kw in ["switch", "gear", "feeder", "arrester", "lightning", "substation"]:
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
                            run_domain_rows: list[dict[str, Any]] = []
                            for inst in device_instances:
                                out_path, layer_name = fill_one_gpkg(
                                    sup_gpkg,
                                    device_choice,
                                    sup_layer,
                                    field_map=inst.get("fields"),
                                    field_order=inst.get("order"),
                                    line_bay_info=line_bay_info,
                                    ups_anchor_info=ups_anchor_info,
                                    type_map=inst.get("type_map") or device_type_map,
                                    sup_wb_path=sup_wb_path,
                                    sup_sheet=sup_sheet,
                                    seq_assign_fallback=seq_assign_fallback,
                                )
                                # create a friendly name per instance
                                label_slug = normalize_for_compare(inst.get("label", "instance")).replace(" ", "_")[:40]
                                fname = f"{Path(sup_gpkg.name).stem}_{label_slug}.gpkg"
                                outputs.append((fname, out_path))
                                # Log domain codes applied for this instance output.
                                run_domain_rows.extend(append_domain_code_log(
                                    _collect_domain_log_entries([inst]),
                                    {
                                        "workbook": sup_wb_path.name if sup_wb_path else None,
                                        "sheet": sup_sheet,
                                        "device": device_choice,
                                        "output": fname,
                                    },
                                ))

                            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as ztmp:
                                zip_path = Path(ztmp.name)
                            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                for fname, out_path in outputs:
                                    zf.write(out_path, arcname=fname)
                                if run_domain_rows:
                                    zf.writestr("domain_code_log.csv", domain_log_rows_to_csv(run_domain_rows))
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
                                type_map=device_type_map,
                                sup_wb_path=sup_wb_path,
                                sup_sheet=sup_sheet,
                                seq_assign_fallback=seq_assign_fallback,
                            )
                            run_domain_rows = append_domain_code_log(
                                _collect_domain_log_entries(device_instances),
                                {
                                    "workbook": sup_wb_path.name if sup_wb_path else None,
                                    "sheet": sup_sheet,
                                    "device": device_choice,
                                    "output": sup_gpkg.name,
                                },
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
                            if run_domain_rows:
                                zip_buf = io.BytesIO()
                                with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                    zf.writestr(Path(sup_gpkg.name).name, data_bytes)
                                    zf.writestr("domain_code_log.csv", domain_log_rows_to_csv(run_domain_rows))
                                st.download_button(
                                    "Download filled GeoPackage + domain log (zip)",
                                    data=zip_buf.getvalue(),
                                    file_name=f"{Path(sup_gpkg.name).stem}_with_domain_log.zip",
                                    mime="application/zip",
                                    key="sup_download_rowmatch_with_log",
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
                                type_map=device_type_map,
                                sup_wb_path=sup_wb_path,
                                sup_sheet=sup_sheet,
                                seq_assign_fallback=seq_assign_fallback,
                            )
                            log_instances = [selected_instance] if selected_instance else device_instances
                            run_domain_rows = append_domain_code_log(
                                _collect_domain_log_entries(log_instances),
                                {
                                    "workbook": sup_wb_path.name if sup_wb_path else None,
                                    "sheet": sup_sheet,
                                    "device": device_choice,
                                    "output": sup_gpkg.name,
                                },
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
                            if run_domain_rows:
                                zip_buf = io.BytesIO()
                                with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                    zf.writestr(Path(sup_gpkg.name).name, data_bytes)
                                    zf.writestr("domain_code_log.csv", domain_log_rows_to_csv(run_domain_rows))
                                st.download_button(
                                    "Download filled GeoPackage + domain log (zip)",
                                    data=zip_buf.getvalue(),
                                    file_name=f"{Path(sup_gpkg.name).stem}_with_domain_log.zip",
                                    mime="application/zip",
                                    key="sup_download_with_log",
                                )
                    except Exception as exc:
                        st.error(f"Supervisor fill failed: {exc}")
            else:
                st.info(f"{len(sup_gpkg_files)} GeoPackages uploaded; the first layer of each will be filled automatically using a per-file device match.")
                if st.button("Fill all uploaded GeoPackages", key="sup_fill_all"):
                    run_id_validation_rows: list[dict[str, Any]] = []
                    outputs, logs, run_domain_rows = fill_supervisor_batch(
                        sup_gpkg_files,
                        device_options,
                        sup_wb_path,
                        sup_sheet,
                        equip_map_sup,
                        line_bay_info,
                        ups_anchor_info,
                        FILL_BATCH_DEPS,
                        seq_assign_fallback=seq_assign_fallback,
                        id_validation_rows=run_id_validation_rows,
                    )

                    if outputs:
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as ztmp:
                            zip_path = Path(ztmp.name)
                        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            for name, out_path in outputs:
                                zf.write(out_path, arcname=name)
                            if run_domain_rows:
                                zf.writestr("domain_code_log.csv", domain_log_rows_to_csv(run_domain_rows))
                            if run_id_validation_rows:
                                zf.writestr(
                                    "rewritten_id_validation_report.csv",
                                    id_validation_rows_to_csv(run_id_validation_rows),
                                )
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
