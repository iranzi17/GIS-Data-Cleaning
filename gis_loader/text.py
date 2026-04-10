import unicodedata
from typing import Any

import pandas as pd

INVISIBLE_HEADER_CHARS = ["\ufeff", "\u200b", "\u200c", "\u200d", "\xa0"]
COMPARISON_IGNORED_CHARS = " -_,./()\\"
COMPARISON_TRANSLATION_TABLE = str.maketrans("", "", COMPARISON_IGNORED_CHARS)


def strip_unicode_spaces(text: str) -> str:
    """Remove all Unicode whitespace including NBSP and thin spaces."""
    if not isinstance(text, str):
        return text
    return "".join(ch for ch in text if unicodedata.category(ch) != "Zs")


def clean_column_name(name: Any) -> str:
    """Normalize header cells while keeping meaningful punctuation."""
    text = "" if name is None else str(name)
    text = "".join(" " if unicodedata.category(ch) == "Zs" else ch for ch in text)
    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")
    text = " ".join(text.split())
    return text.strip()


def ensure_unique_columns(columns: list[str]) -> list[str]:
    """Make duplicate column names unique by appending numeric suffixes."""
    seen: dict[str, int] = {}
    unique: list[str] = []
    for col in columns:
        base = col or ""
        count = seen.get(base, 0) + 1
        seen[base] = count
        unique.append(base if count == 1 else f"{base}_{count}")
    return unique


def normalize_for_compare(name: Any) -> str:
    """Prepare string for comparisons by removing spacing and punctuation noise."""
    if name is None:
        return ""
    text = str(name).lower()
    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")
    text = " ".join(text.split())
    text = text.translate(COMPARISON_TRANSLATION_TABLE)
    return text.strip()


def normalize_value_for_compare(value: Any) -> str:
    """Normalize field values for loose matching."""
    if value is None:
        text = ""
    else:
        try:
            text = "" if pd.isna(value) else str(value)
        except Exception:
            text = str(value)
    for ch in INVISIBLE_HEADER_CHARS:
        text = text.replace(ch, "")
    text = text.lower().replace("_", "").replace("-", "")
    return " ".join(text.split()).strip()

