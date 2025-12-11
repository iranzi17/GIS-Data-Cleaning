from pathlib import Path
import tempfile
import pandas as pd
import geopandas as gpd
import app


def main():
    base = Path('reference_data') / 'GIFURWE'
    excel_path = base / 'GIFURWE DATA.xlsx'
    gpkg_path = base / 'INDOR SWITCHGEAR TABLE.gpkg'
    if not excel_path.exists():
        print('Workbook not found:', excel_path)
        return
    if not gpkg_path.exists():
        print('GPKG not found:', gpkg_path)
        return

    print('Workbook:', excel_path)
    xl = pd.ExcelFile(excel_path)
    print('Sheets:', xl.sheet_names)
    # pick the first sheet that looks like a supervisor sheet
    sheet = xl.sheet_names[0]
    print('Using sheet:', sheet)

    # Determine device name for the gpkg
    equipment_options = app.list_schema_equipments(app.list_reference_workbooks()['Substation Fields.xlsx'], 'Electric device')
    equip_map = app.load_gpkg_equipment_map()
    device_name = app.resolve_equipment_name(gpkg_path.name, equipment_options, equip_map)
    print('Resolved device name for gpkg:', device_name)

    # Parse supervisor instances
    instances = app.parse_supervisor_device_table(excel_path, sheet, device_name)
    print('Found', len(instances), 'instance(s) for device', device_name)
    if instances:
        for i, inst in enumerate(instances[:3], start=1):
            print(f'Instance {i} label:', inst.get('label'))

    # Read gpkg layer (first layer)
    layers = app.list_gpkg_layers(gpkg_path)
    if not layers:
        print('No layers found in gpkg')
        return
    layer = layers[0]
    gdf = gpd.read_file(gpkg_path, layer=layer)
    print('GPKG layer:', layer, 'features:', len(gdf))

    # Simplified fill: if single instance -> apply fields to all rows
    def extract_fields(inst):
        return inst.get('fields', {})

    out_gdf = gdf.copy()

    if not instances:
        print('No instances to fill; exiting')
        return

    if len(instances) == 1:
        fields = extract_fields(instances[0])
        print('Applying', len(fields), 'fields to all features')
        for f, val in fields.items():
            if isinstance(val, pd.Series):
                fill_val = val.iloc[0] if not val.empty else pd.NA
            else:
                fill_val = val
            out_gdf[f] = fill_val
    else:
        # try to find a match column in gdf by checking common id/name columns
        candidate_cols = [c for c in out_gdf.columns if c != out_gdf.geometry.name]
        print('Candidate match columns in GPKG:', candidate_cols)
        # build mapping from normalized supervisor id/name/feeder -> fields
        inst_map = {}
        for inst in instances:
            idv = inst.get('id_value')
            namev = inst.get('name_value')
            feedv = inst.get('feeder_value')
            keys = [idv, namev, feedv]
            for k in keys:
                norm = app.normalize_value_for_compare(k)
                if norm:
                    inst_map[norm] = extract_fields(inst)
        print('Built', len(inst_map), 'normalized instance keys')

        # pick best candidate column to match by checking overlap
        best_col = None
        best_hits = -1
        for col in candidate_cols:
            hits = 0
            for val in out_gdf[col].fillna('').astype(str):
                if app.normalize_value_for_compare(val) in inst_map:
                    hits += 1
            if hits > best_hits:
                best_hits = hits
                best_col = col
        print('Best match column:', best_col, 'hits:', best_hits)

        if best_col and best_hits > 0:
            # fill rows by matching
            for idx, row in out_gdf.iterrows():
                key = app.normalize_value_for_compare(row[best_col])
                fields = inst_map.get(key)
                if not fields:
                    continue
                for f, val in fields.items():
                    if isinstance(val, pd.Series):
                        fill_val = val.iloc[0] if not val.empty else pd.NA
                    else:
                        fill_val = val
                    out_gdf.at[idx, f] = fill_val
        else:
            # fallback: if counts match, assign instances sequentially
            if len(instances) == len(out_gdf):
                print('Assigning instances sequentially to rows')
                for idx, inst in enumerate(instances):
                    fields = extract_fields(inst)
                    for f, val in fields.items():
                        if isinstance(val, pd.Series):
                            fill_val = val.iloc[0] if not val.empty else pd.NA
                        else:
                            fill_val = val
                        out_gdf.at[idx, f] = fill_val
            else:
                print('No reliable match; applying first instance as default')
                fields = extract_fields(instances[0])
                for f, val in fields.items():
                    if isinstance(val, pd.Series):
                        fill_val = val.iloc[0] if not val.empty else pd.NA
                    else:
                        fill_val = val
                    out_gdf[f] = fill_val

    # Only keep geometry + filled fields to avoid schema issues with original columns
    geom_name = out_gdf.geometry.name if hasattr(out_gdf, 'geometry') else 'geometry'
    filled_cols = [c for c in out_gdf.columns if c != geom_name and c not in [geom_name]]
    keep_cols = [geom_name] + filled_cols if geom_name in out_gdf.columns else filled_cols
    out_trim = out_gdf[keep_cols].copy() if keep_cols else out_gdf.copy()
    # Inspect dtypes before writing
    print('\nPrepared output columns and dtypes:')
    for c, dt in out_trim.dtypes.items():
        print(' -', c, dt)
    print('\nSample values (first 5 rows):')
    try:
        print(out_trim.head(5).to_dict(orient='records'))
    except Exception:
        pass

    # Normalize column names to lower-case underscore form to avoid case-only duplicates
    geom_name = out_trim.geometry.name if hasattr(out_trim, 'geometry') else None
    new_cols = []
    for col in out_trim.columns:
        if geom_name and col == geom_name:
            new_cols.append(col)
            continue
        cleaned = app._clean_column_name(col)
        cleaned = cleaned.replace(' ', '_').strip().lower()
        # truncate to a safe length
        if len(cleaned) > 200:
            cleaned = cleaned[:200]
        new_cols.append(cleaned)
    out_trim.columns = app.ensure_unique_columns(new_cols)
    out_trim = app.sanitize_gdf_for_gpkg(out_trim)
    with tempfile.NamedTemporaryFile(suffix='.gpkg', delete=False) as tmp:
        out_path = Path(tmp.name)
    out_trim.to_file(out_path, driver='GPKG', layer=layer)
    print('Wrote filled GPKG to', out_path)


if __name__ == '__main__':
    main()
