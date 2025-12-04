import warnings, zipfile, tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import geopandas as gpd
import app

warnings.filterwarnings("ignore", category=UserWarning)

def main():
    zip_path = Path('reference_data/MUSHA.zip')
    schema_files = app.list_reference_workbooks()
    schema_label = 'Substation Fields.xlsx' if 'Substation Fields.xlsx' in schema_files else list(schema_files.keys())[0]
    schema_path = schema_files[schema_label]
    schema_sheet = 'Electric device'
    equipment_options = app.list_schema_equipments(schema_path, schema_sheet)
    equip_map = app.load_gpkg_equipment_map()
    mapping_threshold_auto = 0.35
    keep_unmatched_auto = True
    accept_threshold = 0.5

    with tempfile.TemporaryDirectory() as tmp_in_str, tempfile.TemporaryDirectory() as tmp_out_str:
        tmp_in = Path(tmp_in_str); tmp_out = Path(tmp_out_str)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmp_in)
        gpkg_paths = sorted(tmp_in.rglob('*.gpkg'))
        args = [(
            gpkg,
            equipment_options,
            equip_map,
            schema_path,
            schema_sheet,
            mapping_threshold_auto,
            keep_unmatched_auto,
            accept_threshold,
            str(tmp_out),
        ) for gpkg in gpkg_paths]
        max_workers = min(4, len(args) or 1)
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            results = list(ex.map(app.process_single_gpkg, args))
        logs = []
        for out_path, msg in results:
            logs.append(msg)
        print('Processed:', len(results), 'files')
        print('Log sample:', logs[:5])
        out_files = list(tmp_out.glob('*.gpkg'))
        summary = []
        for of in sorted(out_files):
            layers = app.list_gpkg_layers(of)
            if not layers:
                summary.append((of.name, 'no layers', None))
                continue
            gdf = gpd.read_file(of, layer=layers[0])
            null_total = int(gdf.isna().sum().sum())
            summary.append((of.name, len(gdf), null_total))
        print('Output sample:', summary[:6])

if __name__ == '__main__':
    main()
