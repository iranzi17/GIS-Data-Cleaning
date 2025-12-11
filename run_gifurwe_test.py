import tempfile
from pathlib import Path
import app


def main():
    gpkg_dir = Path("reference_data/GIFURWE")
    if not gpkg_dir.exists():
        print("GIFURWE folder not found:", gpkg_dir)
        return

    schema_files = app.list_reference_workbooks()
    if "Substation Fields.xlsx" in schema_files:
        schema_path = schema_files["Substation Fields.xlsx"]
    elif schema_files:
        schema_path = list(schema_files.values())[0]
    else:
        print("No schema workbooks found in reference_data. Aborting.")
        return

    schema_sheet = "Electric device"
    equipment_options = app.list_schema_equipments(schema_path, schema_sheet)
    equip_map = app.load_gpkg_equipment_map()
    mapping_threshold_auto = 0.35
    keep_unmatched_auto = True
    accept_threshold = 0.5

    tmp_out = Path(tempfile.mkdtemp())
    print("Using schema:", schema_path)
    print("Temporary output dir:", tmp_out)

    results = []
    for gpkg in sorted(gpkg_dir.rglob("*.gpkg")):
        print("Processing:", gpkg)
        args = (
            str(gpkg),
            equipment_options,
            equip_map,
            schema_path,
            schema_sheet,
            mapping_threshold_auto,
            keep_unmatched_auto,
            accept_threshold,
            str(tmp_out),
        )
        out_path, msg = app.process_single_gpkg(args)
        print(msg)
        results.append((out_path, msg))

    print("Processed:", len(results), "files")
    out_files = list(tmp_out.glob("*.gpkg"))
    if out_files:
        print("Output files:")
        for p in sorted(out_files):
            print(" -", p.name)
    else:
        print("No output files generated in", tmp_out)


if __name__ == "__main__":
    main()
