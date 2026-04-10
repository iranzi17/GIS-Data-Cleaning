from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
REFERENCE_DATA_DIR = BASE_DIR / "reference_data"
SUPERVISOR_WORKBOOK_DIR = BASE_DIR / "supervisor_workbooks"
NEW_DATA_DIR = BASE_DIR / "New Data"
SUPERVISOR_WORKBOOK_DIRS = [NEW_DATA_DIR, SUPERVISOR_WORKBOOK_DIR]

# Preferred workbook order: newest first; falls back to any available in reference_data.
WORKBOOK_PRIORITY = [
    "SUBSTATION 1-25102025.xlsx",
    "SUBSTATIONS 2-25112025.xlsx",
    "SUBSTATIONS 2-251025.xlsx",
]
WORKBOOK_NAME = WORKBOOK_PRIORITY[0]
WORKBOOK_PATH = REFERENCE_DATA_DIR / WORKBOOK_NAME

REFERENCE_EXTENSIONS = (".xlsx", ".xlsm")
ALIAS_FILE = REFERENCE_DATA_DIR / "alias_map.json"
GPKG_EQUIP_MAP_FILE = REFERENCE_DATA_DIR / "gpkg_equipment_map.json"
MAPPING_CACHE_FILE = REFERENCE_DATA_DIR / "schema_mapping_cache.json"
DOMAIN_CODE_CACHE_FILE = REFERENCE_DATA_DIR / "domain_code_cache.json"
DOMAIN_CODE_LOG_FILE = REFERENCE_DATA_DIR / "domain_code_log.jsonl"

TEMPLATE_DIR = BASE_DIR / "For High Voltage Line"
HV_LINE_TEMPLATE_PATH = TEMPLATE_DIR / "High Voltage Lines.gpkg"
EARTHING_TRANSFORMER_TEMPLATE_PATH = TEMPLATE_DIR / "EARTHING TRANSFORMER.gpkg"
LINE_BAY_LIBRARY_DIR = BASE_DIR / "For Line Bays"
LINE_BAY_LIBRARY_PATH = LINE_BAY_LIBRARY_DIR / "LINE BAY.gpkg"

PREVIEW_ROWS = 30
MAX_GPKG_NAME_LENGTH = 254

# Curated equipment names from the "Electric device" schema sheet (hard-coded for stability/order).
ELECTRIC_DEVICE_EQUIPMENT = [
    "Power Transformer/ Stepup Transformer",
    "Power Transformer",
    "Distribution Transformer",
    "Earthing Transformer",
    "High Voltage Busbar/Medium Voltage Busbar",
    "MV Switch gear",
    "Line Bay",
    "Voltage Transformer",
    "Current Transformer",
    "High Voltage Circuit Breaker/High Voltage Circuit Breaker",
    "High Voltage Switch/High Voltage Switch",
    "Uninterruptable power supply(UPS)",
    "Substation/Cabin",
    "Optical Telecommunication Equipment (Telecom)",
    "ODF",
    "Lightning Arrester",
    "DC Supply 48 VDC Battery",
    "DC Supply 110 VDC Battery",
    "DC Supply 48 VDC charger",
    "DC Supply 110 VDC charger",
    "DIGITAL fault recorder",
    "High Voltage Line",
    "Transformer Bay",
    "Indoor Circuit Breaker/30kv/15kb",
    "Indoor Current Transformer",
    "Indoor Voltage Transformer",
    "Control and Protection Panels",
    "Distance Protection",
    "Transformer Protection",
    "Line Overcurrent Protection",
    "Standby Generator",
]

