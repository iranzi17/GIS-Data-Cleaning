from pathlib import Path
import tempfile
import pandas as pd
import app


def find_excel_files(ref_dir: Path):
    for p in sorted(ref_dir.glob("*.xlsx")):
        # skip the schema workbooks
        if p.name.lower().startswith("substation") or p.name.lower().startswith("substations"):
            continue
        yield p


def main():
    ref = Path("reference_data")
    if not ref.exists():
        print("reference_data not found")
        return

    # pick schema workbook
    schema_files = app.list_reference_workbooks()
    if "Substation Fields.xlsx" in schema_files:
        schema_path = schema_files["Substation Fields.xlsx"]
    elif schema_files:
        schema_path = list(schema_files.values())[0]
    else:
        print("No schema workbook found")
        return

    equipment_options = app.list_schema_equipments(schema_path, "Electric device")
    print("Using schema:", schema_path.name)
    print("Equipment options (sample):", equipment_options[:6])

    results = []
    for excel in find_excel_files(ref):
        print('\n===', excel.name, '===')
        try:
            xl = pd.ExcelFile(excel)
            print('Sheets:', xl.sheet_names)
        except Exception as e:
            print('Failed to read:', excel, e)
            continue

        # try sheets that look like supervisor sheets
        candidate_sheets = [s for s in xl.sheet_names if 'device' in s.lower() or 'electric' in s.lower() or 'supervisor' in s.lower()]
        if not candidate_sheets:
            candidate_sheets = xl.sheet_names[:2]

        for sheet in candidate_sheets:
            print('\n-- Sheet:', sheet)
            for device in equipment_options[:12]:
                try:
                    instances = app.parse_supervisor_device_table(excel, sheet, device)
                    count = len(instances) if instances is not None else 0
                    if count:
                        print(f"Device: {device!r} -> {count} instance(s)")
                        # show first instance sample
                        inst = instances[0]
                        sample_fields = list(inst.get('fields', {}).items())[:6]
                        print(' Sample fields:', sample_fields)
                        results.append((excel.name, sheet, device, count))
                except Exception as e:
                    print(f"Parsing failed for device {device!r}: {e}")
    print('\nSummary:')
    for r in results:
        print('-', r)

if __name__ == '__main__':
    main()
