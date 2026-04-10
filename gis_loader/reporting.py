import csv
import io
import json
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as pd

        return bool(pd.isna(value))
    except Exception:
        return False


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if _is_missing(value):
        return None
    try:
        if hasattr(value, "item") and not isinstance(value, (str, bytes)):
            return _jsonable(value.item())
    except Exception:
        pass
    return value


def rows_to_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        return b""
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            key_str = str(key)
            if key_str not in seen:
                seen.add(key_str)
                fieldnames.append(key_str)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _jsonable(row.get(key)) for key in fieldnames})
    return buf.getvalue().encode("utf-8")


def _safe_section_name(name: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(name).strip())
    text = text.strip("._")
    return text or "section"


@dataclass
class ReportEvent:
    timestamp: str
    level: str
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        row = {
            "timestamp": self.timestamp,
            "level": self.level,
            "code": self.code,
            "message": self.message,
        }
        for key, value in self.context.items():
            row[f"context_{key}"] = _jsonable(value)
        return row


@dataclass
class ReportValidation:
    timestamp: str
    rule: str
    status: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        row = {
            "timestamp": self.timestamp,
            "rule": self.rule,
            "status": self.status,
            "message": self.message,
        }
        for key, value in self.context.items():
            row[f"context_{key}"] = _jsonable(value)
        for key, value in self.metrics.items():
            row[f"metric_{key}"] = _jsonable(value)
        return row


@dataclass
class ReportArtifact:
    timestamp: str
    kind: str
    name: str
    path: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        row = {
            "timestamp": self.timestamp,
            "kind": self.kind,
            "name": self.name,
            "path": self.path or "",
        }
        for key, value in self.details.items():
            row[f"detail_{key}"] = _jsonable(value)
        return row


@dataclass
class RunReport:
    workflow: str
    run_id: str | None = None
    title: str | None = None
    started_at: str = field(default_factory=_iso_now)
    metadata: dict[str, Any] = field(default_factory=dict)
    events: list[ReportEvent] = field(default_factory=list)
    validations: list[ReportValidation] = field(default_factory=list)
    artifacts: list[ReportArtifact] = field(default_factory=list)
    sections: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    child_summaries: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.run_id is None:
            slug = _safe_section_name(self.workflow).replace(".", "_")
            self.run_id = f"{slug}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    def set_metadata(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            self.metadata[key] = _jsonable(value)

    def add_event(self, level: str, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.events.append(
            ReportEvent(
                timestamp=_iso_now(),
                level=level,
                code=code,
                message=message,
                context=_jsonable(context or {}),
            )
        )

    def info(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.add_event("info", code, message, context)

    def warning(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.add_event("warning", code, message, context)

    def error(self, code: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.add_event("error", code, message, context)

    def exception(self, code: str, exc: Exception, context: dict[str, Any] | None = None) -> None:
        details = dict(context or {})
        details["exception_type"] = type(exc).__name__
        self.error(code, f"{type(exc).__name__}: {exc}", details)

    def add_validation(
        self,
        rule: str,
        status: str,
        message: str,
        context: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        self.validations.append(
            ReportValidation(
                timestamp=_iso_now(),
                rule=rule,
                status=status,
                message=message,
                context=_jsonable(context or {}),
                metrics=_jsonable(metrics or {}),
            )
        )

    def add_validation_result(self, result: dict[str, Any]) -> None:
        self.add_validation(
            rule=str(result.get("rule") or result.get("name") or "validation"),
            status=str(result.get("status") or "info"),
            message=str(result.get("message") or ""),
            context=result.get("context") or {},
            metrics=result.get("metrics") or {},
        )

    def add_artifact(
        self,
        kind: str,
        name: str,
        path: str | Path | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details_local = dict(details or {})
        if path is not None:
            p = Path(path)
            details_local.setdefault("exists", p.exists())
            if p.exists():
                try:
                    details_local.setdefault("size_bytes", p.stat().st_size)
                except Exception:
                    pass
            path = str(p)
        self.artifacts.append(
            ReportArtifact(
                timestamp=_iso_now(),
                kind=kind,
                name=name,
                path=path,
                details=_jsonable(details_local),
            )
        )

    def add_section_rows(self, name: str, rows: list[dict[str, Any]] | None) -> None:
        if not rows:
            return
        key = _safe_section_name(name)
        self.sections.setdefault(key, []).extend(_jsonable(rows))

    def extend(self, other: "RunReport", section_prefix: str | None = None) -> None:
        self.events.extend(other.events)
        self.validations.extend(other.validations)
        self.artifacts.extend(other.artifacts)
        self.child_summaries.append(other.summary())
        prefix = _safe_section_name(section_prefix) if section_prefix else None
        for name, rows in other.sections.items():
            merged_name = f"{prefix}__{name}" if prefix else name
            self.sections.setdefault(merged_name, []).extend(rows)

    def summary(self) -> dict[str, Any]:
        event_counts = Counter(event.level for event in self.events)
        validation_counts = Counter(validation.status for validation in self.validations)
        return {
            "run_id": self.run_id,
            "workflow": self.workflow,
            "title": self.title or self.workflow,
            "started_at": self.started_at,
            "event_count": len(self.events),
            "validation_count": len(self.validations),
            "artifact_count": len(self.artifacts),
            "section_count": len(self.sections),
            "info_count": event_counts.get("info", 0),
            "warning_count": event_counts.get("warning", 0),
            "error_count": event_counts.get("error", 0),
            "validation_pass_count": validation_counts.get("pass", 0),
            "validation_warn_count": validation_counts.get("warn", 0),
            "validation_fail_count": validation_counts.get("fail", 0),
        }

    def log_lines(self) -> list[str]:
        lines: list[str] = []
        for event in self.events:
            prefix = "" if event.level == "info" else f"[{event.level.upper()}] "
            lines.append(f"{prefix}{event.message}")
        return lines

    def log_text(self) -> str:
        lines = self.log_lines()
        return "\n".join(lines) if lines else "No logs."

    def events_rows(self) -> list[dict[str, Any]]:
        return [event.to_row() for event in self.events]

    def validations_rows(self) -> list[dict[str, Any]]:
        return [validation.to_row() for validation in self.validations]

    def artifacts_rows(self) -> list[dict[str, Any]]:
        return [artifact.to_row() for artifact in self.artifacts]

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary(),
            "metadata": _jsonable(self.metadata),
            "events": self.events_rows(),
            "validations": self.validations_rows(),
            "artifacts": self.artifacts_rows(),
            "sections": _jsonable(self.sections),
            "child_summaries": _jsonable(self.child_summaries),
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")


def build_report_bundle(
    report: RunReport,
    *,
    extra_files: dict[str, str | bytes] | None = None,
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("run_report.json", report.to_json_bytes())
        zf.writestr("summary.csv", rows_to_csv_bytes([report.summary()]))
        if report.events:
            zf.writestr("events.csv", rows_to_csv_bytes(report.events_rows()))
        if report.validations:
            zf.writestr("validations.csv", rows_to_csv_bytes(report.validations_rows()))
        if report.artifacts:
            zf.writestr("artifacts.csv", rows_to_csv_bytes(report.artifacts_rows()))
        if report.child_summaries:
            zf.writestr("child_summaries.csv", rows_to_csv_bytes(report.child_summaries))
        for name, rows in report.sections.items():
            zf.writestr(f"sections/{_safe_section_name(name)}.csv", rows_to_csv_bytes(rows))
        for name, content in (extra_files or {}).items():
            if isinstance(content, str):
                zf.writestr(name, content.encode("utf-8"))
            else:
                zf.writestr(name, content)
    return buf.getvalue()
