from typing import Any


class GISLoaderError(Exception):
    """Base application error carrying a stable machine-readable code."""

    def __init__(self, message: str, *, code: str = "gis_loader_error", details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "details": self.details,
        }


class DatasetReadError(GISLoaderError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="dataset_read_error", details=details)


class WorkbookLookupError(GISLoaderError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="workbook_lookup_error", details=details)


class MappingError(GISLoaderError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="mapping_error", details=details)


class DataValidationError(GISLoaderError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code="data_validation_error", details=details)
