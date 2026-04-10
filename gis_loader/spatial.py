import math
import re
import statistics
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import streamlit as st

from .data_sources import coerce_gpkg_path as _coerce_gpkg_path
from .data_sources import get_file_name as _get_file_name
from .data_sources import list_gpkg_layers
from .equipment import resolve_equipment_name
from .supervisor import parse_supervisor_device_table
from .text import normalize_for_compare, normalize_value_for_compare

PROTECTION_LAYOUT_SPACING = 2.0
_NUM_REGEX = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def order_indices_by_location(geom: gpd.GeoSeries) -> list[int]:
    """Return geometry indices ordered by location using a dominant-axis sort with band grouping."""
    if geom is None:
        return []
    coords: list[tuple[int, float, float]] = []
    missing: list[int] = []
    for idx, g in geom.items():
        if g is None or getattr(g, "is_empty", True):
            missing.append(idx)
            continue
        try:
            pt = g if getattr(g, "geom_type", "") == "Point" else g.centroid
        except Exception:
            missing.append(idx)
            continue
        if pt is None or getattr(pt, "is_empty", True):
            missing.append(idx)
            continue
        try:
            x = float(pt.x)
            y = float(pt.y)
        except Exception:
            missing.append(idx)
            continue
        coords.append((idx, x, y))

    if len(coords) <= 1:
        return [idx for idx, _, _ in coords] + missing

    xs = [x for _, x, _ in coords]
    ys = [y for _, _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]

    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)

    if var_x < 1e-12 and var_y < 1e-12:
        ordered = sorted(coords, key=lambda t: (t[2], t[1]))
        return [idx for idx, _, _ in ordered] + missing

    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)

    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0

    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm

    # Orient axis to keep ordering stable (north/east positive).
    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy

    along_perp: list[tuple[int, float, float]] = []
    for idx, x, y in coords:
        dx = x - mean_x
        dy = y - mean_y
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        along_perp.append((idx, along, perp))

    perp_sorted = sorted(along_perp, key=lambda t: t[2])
    perps = [p for _, _, p in perp_sorted]
    if len(perps) < 2:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    diffs = [perps[i + 1] - perps[i] for i in range(len(perps) - 1)]
    diffs_sorted = sorted(diffs)
    median_diff = diffs_sorted[len(diffs_sorted) // 2]
    abs_dev = [abs(d - median_diff) for d in diffs_sorted]
    mad = sorted(abs_dev)[len(abs_dev) // 2] if abs_dev else 0.0
    # Only split when gaps are clearly larger than the typical spacing.
    gap_threshold = max(median_diff * 3, median_diff + 3 * mad)

    if gap_threshold <= 0:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    groups: list[list[tuple[int, float, float]]] = []
    current: list[tuple[int, float, float]] = []
    last_perp: float | None = None
    for item in perp_sorted:
        if last_perp is None:
            current = [item]
        elif item[2] - last_perp > gap_threshold:
            groups.append(current)
            current = [item]
        else:
            current.append(item)
        last_perp = item[2]
    if current:
        groups.append(current)

    if len(groups) <= 1:
        ordered = sorted(along_perp, key=lambda t: t[1])
        return [idx for idx, _, _ in ordered] + missing

    def _group_median(group: list[tuple[int, float, float]]) -> float:
        return statistics.median([p[2] for p in group])

    ordered_indices: list[int] = []
    for group in sorted(groups, key=_group_median):
        group_sorted = sorted(group, key=lambda t: t[1])
        ordered_indices.extend([idx for idx, _, _ in group_sorted])

    return ordered_indices + missing

def group_indices_by_perp_gap(geom: gpd.GeoSeries, group_count: int) -> dict[int, int]:
    """Group geometry indices into contiguous bands based on perpendicular gaps."""
    if geom is None or group_count <= 0:
        return {}

    coords: list[tuple[int, float, float]] = []
    missing: list[int] = []
    for idx, g in geom.items():
        if g is None or getattr(g, "is_empty", True):
            missing.append(idx)
            continue
        try:
            pt = g if getattr(g, "geom_type", "") == "Point" else g.centroid
        except Exception:
            missing.append(idx)
            continue
        if pt is None or getattr(pt, "is_empty", True):
            missing.append(idx)
            continue
        try:
            x = float(pt.x)
            y = float(pt.y)
        except Exception:
            missing.append(idx)
            continue
        coords.append((idx, x, y))

    if len(coords) <= 1:
        mapping = {idx: 0 for idx, _, _ in coords}
        for idx in missing:
            mapping[idx] = 0
        return mapping

    xs = [x for _, x, _ in coords]
    ys = [y for _, _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]

    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)

    if var_x < 1e-12 and var_y < 1e-12:
        ordered = sorted(coords, key=lambda t: (t[2], t[1]))
        mapping = {idx: 0 for idx, _, _ in ordered}
        for idx in missing:
            mapping[idx] = 0
        return mapping

    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)

    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0

    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm

    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy

    items: list[tuple[int, float, float]] = []
    for idx, x, y in coords:
        dx = x - mean_x
        dy = y - mean_y
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        items.append((idx, along, perp))

    items.sort(key=lambda t: t[2])
    group_count = max(1, min(group_count, len(items)))

    groups: list[list[tuple[int, float, float]]] = [[item] for item in items]
    # Merge the closest neighboring bands until we have the requested count.
    while len(groups) > group_count:
        gaps = [
            groups[i + 1][0][2] - groups[i][-1][2]
            for i in range(len(groups) - 1)
        ]
        merge_idx = gaps.index(min(gaps))
        groups[merge_idx].extend(groups[merge_idx + 1])
        del groups[merge_idx + 1]

    mapping: dict[int, int] = {}
    for group_id, group in enumerate(groups):
        group_sorted = sorted(group, key=lambda t: t[1])
        for idx, _, _ in group_sorted:
            mapping[idx] = group_id
    for idx in missing:
        mapping[idx] = min(group_count - 1, len(groups) - 1)
    return mapping

def resolve_ups_anchor_point(ups_path: Path, ups_layer: str | None, target_crs) -> Any:
    """Return a Point anchor from an UPS GeoPackage layer."""
    if ups_path is None or ups_layer is None:
        return None
    try:
        ups_gdf = gpd.read_file(ups_path, layer=ups_layer)
    except Exception:
        return None
    if ups_gdf.empty or not hasattr(ups_gdf, "geometry"):
        return None
    if target_crs is not None and ups_gdf.crs is not None and ups_gdf.crs != target_crs:
        try:
            ups_gdf = ups_gdf.to_crs(target_crs)
        except Exception:
            pass
    for geom in ups_gdf.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        try:
            return geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
        except Exception:
            continue
    return None

def load_ups_anchor_and_crs(ups_path: Path, ups_layer: str | None) -> tuple[Any, Any]:
    """Return a Point anchor and CRS from an UPS GeoPackage layer."""
    if ups_path is None or ups_layer is None:
        return None, None
    try:
        ups_gdf = gpd.read_file(ups_path, layer=ups_layer)
    except Exception:
        return None, None
    if ups_gdf.empty or not hasattr(ups_gdf, "geometry"):
        return None, ups_gdf.crs
    for geom in ups_gdf.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        try:
            anchor = geom if getattr(geom, "geom_type", "") == "Point" else geom.centroid
            return anchor, ups_gdf.crs
        except Exception:
            continue
    return None, ups_gdf.crs

def build_protection_layout_points(anchor: Any, count: int, spacing: float) -> list[Any]:
    """Build protection points in a 2xN grid below the anchor point."""
    if anchor is None or count <= 0:
        return []
    try:
        x = float(anchor.x)
        y = float(anchor.y)
    except Exception:
        return []
    if spacing <= 0:
        spacing = PROTECTION_LAYOUT_SPACING
    try:
        from shapely.geometry import Point
    except Exception:
        return []

    points: list[Any] = []
    if count == 1:
        points.append(Point(x, y - spacing))
        return points
    if count == 2:
        points.append(Point(x - spacing * 0.5, y - spacing))
        points.append(Point(x + spacing * 0.5, y - spacing))
        return points

    for i in range(count):
        row = i // 2
        col = i % 2
        x_off = (-0.5 + col) * spacing
        y_off = -(row + 1) * spacing
        points.append(Point(x + x_off, y + y_off))
    return points

def _normalize_length_to_meters(value: Any, field_name: str | None = None) -> float | None:
    if value is None:
        return None
    num = _extract_first_number(value)
    if num is None:
        return None
    norm = normalize_for_compare(field_name or "")
    if "mm" in norm:
        return num / 1000.0
    if "cm" in norm:
        return num / 100.0
    if "meter" in norm or norm.endswith("m"):
        return num
    # Heuristic: large values likely in mm.
    if num > 20:
        return num / 1000.0
    return num

def _extract_length_from_fields(fields: dict[str, Any], keywords: list[str]) -> tuple[float | None, str | None]:
    for key, val in (fields or {}).items():
        norm_key = normalize_for_compare(key)
        if any(k in norm_key for k in keywords):
            num = _extract_first_number(val)
            if num is not None:
                return num, key
    return None, None

def _default_panel_size_from_poly(poly: Any) -> tuple[float, float]:
    default_w = 1.5
    default_d = 1.0
    if poly is None or getattr(poly, "is_empty", True):
        return default_w, default_d
    try:
        minx, miny, maxx, maxy = poly.bounds
        bbox_w = maxx - minx
        bbox_h = maxy - miny
        if bbox_w > 0:
            default_w = max(default_w, bbox_w * 0.2)
        if bbox_h > 0:
            default_d = max(default_d, bbox_h * 0.2)
    except Exception:
        pass
    return default_w, default_d

def _layout_points_in_cabins(
    cabins_gdf: gpd.GeoDataFrame | None,
    count: int,
    anchors: list[Any] | None = None,
) -> list[tuple[Any, Any]]:
    """Return (point, cabin_polygon) pairs for placing cabin-interior devices."""
    if count <= 0 or cabins_gdf is None or cabins_gdf.empty:
        return []
    try:
        from shapely.geometry import Point
    except Exception:
        return []
    cabin_polys = list(cabins_gdf.geometry)
    if not cabin_polys:
        return []
    n_cabins = len(cabin_polys)
    base = count // n_cabins
    remainder = count % n_cabins
    counts = [base + (1 if i < remainder else 0) for i in range(n_cabins)]
    out: list[tuple[Any, Any]] = []
    for idx, poly in enumerate(cabin_polys):
        c = counts[idx]
        if c <= 0:
            continue
        anchor = None
        if anchors and idx < len(anchors):
            anchor = anchors[idx]
        if anchor is None and poly is not None:
            try:
                anchor = poly.centroid
            except Exception:
                anchor = None
        if anchor is None and poly is not None:
            try:
                anchor = poly.representative_point()
            except Exception:
                anchor = None
        if c == 1:
            if anchor is not None:
                out.append((anchor, poly))
            continue
        try:
            minx, miny, maxx, maxy = poly.bounds
            bbox_w = maxx - minx
            bbox_h = maxy - miny
        except Exception:
            bbox_w = bbox_h = 0.0
        if bbox_w <= 0 or bbox_h <= 0:
            if anchor is not None:
                out.extend([(anchor, poly)] * c)
            continue
        cols = int(math.ceil(math.sqrt(c)))
        rows = int(math.ceil(c / cols))
        dx = bbox_w / (cols + 1)
        dy = bbox_h / (rows + 1)
        points: list[Any] = []
        for r in range(rows):
            for col in range(cols):
                if len(points) >= c:
                    break
                x = minx + (col + 1) * dx
                y = maxy - (r + 1) * dy
                pt = Point(x, y)
                try:
                    if poly is not None and not poly.contains(pt):
                        try:
                            pt = poly.representative_point()
                        except Exception:
                            pt = anchor if anchor is not None else pt
                except Exception:
                    pt = anchor if anchor is not None else pt
                points.append(pt)
        out.extend([(pt, poly) for pt in points])
    return out

def layout_points_in_cabins(
    cabins_gdf: gpd.GeoDataFrame | None,
    count: int,
    anchors: list[Any] | None = None,
) -> list[Any]:
    return [pt for pt, _ in _layout_points_in_cabins(cabins_gdf, count, anchors)]

def build_cabin_anchor_points(
    files: list[Any] | None,
    cabins_gdf: gpd.GeoDataFrame | None,
    device_options: list[str],
    equip_map: dict[str, str],
) -> list[Any]:
    """Find anchor points inside each cabin (prefer switchgear points, fall back to centroids)."""
    if cabins_gdf is None or cabins_gdf.empty:
        return []
    switchgear_norms = {
        normalize_for_compare("MV Switch gear"),
        normalize_for_compare("INDOR SWITCHGEAR TABLE"),
    }
    switchgear_pts = collect_device_points_from_uploads(
        files,
        cabins_gdf.crs,
        device_options,
        equip_map,
        switchgear_norms,
    )
    try:
        if (
            switchgear_pts is not None
            and not switchgear_pts.empty
            and switchgear_pts.crs is not None
            and cabins_gdf.crs is not None
            and switchgear_pts.crs != cabins_gdf.crs
        ):
            switchgear_pts = switchgear_pts.to_crs(cabins_gdf.crs)
    except Exception:
        pass
    anchors: list[Any] = []
    for _, cabin in cabins_gdf.iterrows():
        poly = cabin.geometry
        anchor = None
        if switchgear_pts is not None and not switchgear_pts.empty:
            try:
                pts_inside = switchgear_pts[switchgear_pts.within(poly)]
            except Exception:
                pts_inside = gpd.GeoDataFrame()
            if not pts_inside.empty:
                try:
                    anchor = pts_inside.unary_union.centroid
                except Exception:
                    anchor = pts_inside.iloc[0].geometry
        if anchor is None:
            try:
                anchor = poly.centroid
            except Exception:
                anchor = None
        anchors.append(anchor)
    return anchors

def _build_panel_rectangle(
    center: Any,
    width_m: float,
    depth_m: float,
    cabin_poly: Any | None = None,
) -> Any:
    try:
        from shapely.geometry import Polygon
    except Exception:
        return None
    if center is None or getattr(center, "is_empty", True):
        if cabin_poly is not None:
            try:
                center = cabin_poly.centroid
            except Exception:
                return None
        else:
            return None
    try:
        x = float(center.x)
        y = float(center.y)
    except Exception:
        return None
    half_w = max(width_m, 0.2) / 2.0
    half_d = max(depth_m, 0.2) / 2.0
    rect = Polygon(
        [
            (x - half_w, y - half_d),
            (x + half_w, y - half_d),
            (x + half_w, y + half_d),
            (x - half_w, y + half_d),
        ]
    )
    if cabin_poly is None:
        return rect
    try:
        if cabin_poly.contains(rect):
            return rect
    except Exception:
        return rect
    for factor in (0.9, 0.8, 0.6, 0.4):
        rect_try = Polygon(
            [
                (x - half_w * factor, y - half_d * factor),
                (x + half_w * factor, y - half_d * factor),
                (x + half_w * factor, y + half_d * factor),
                (x - half_w * factor, y + half_d * factor),
            ]
        )
        try:
            if cabin_poly.contains(rect_try):
                return rect_try
        except Exception:
            continue
    return rect

def _build_oriented_panel_rectangle(
    center_along: float,
    center_perp: float,
    width_m: float,
    depth_m: float,
    ux: float,
    uy: float,
    px: float,
    py: float,
    cx: float,
    cy: float,
    cabin_poly: Any | None = None,
) -> Any:
    """Build a rectangle oriented by cabin dominant axis, shrinking if needed to stay inside."""
    try:
        from shapely.geometry import Polygon
    except Exception:
        return None

    half_w = max(width_m, 0.2) / 2.0
    half_d = max(depth_m, 0.2) / 2.0

    def _to_world(along: float, perp: float) -> tuple[float, float]:
        x = cx + along * ux + perp * px
        y = cy + along * uy + perp * py
        return float(x), float(y)

    rect = Polygon(
        [
            _to_world(center_along - half_w, center_perp - half_d),
            _to_world(center_along + half_w, center_perp - half_d),
            _to_world(center_along + half_w, center_perp + half_d),
            _to_world(center_along - half_w, center_perp + half_d),
        ]
    )
    if cabin_poly is None:
        return rect
    try:
        if cabin_poly.contains(rect):
            return rect
    except Exception:
        return rect

    for factor in (0.9, 0.8, 0.7, 0.6, 0.5, 0.4):
        w_try = half_w * factor
        d_try = half_d * factor
        rect_try = Polygon(
            [
                _to_world(center_along - w_try, center_perp - d_try),
                _to_world(center_along + w_try, center_perp - d_try),
                _to_world(center_along + w_try, center_perp + d_try),
                _to_world(center_along - w_try, center_perp + d_try),
            ]
        )
        try:
            if cabin_poly.contains(rect_try):
                return rect_try
        except Exception:
            continue
    return rect

def build_points_in_panel_polygons(panel_polygons: list[Any] | None, count: int) -> list[Any]:
    """Create point(s) inside panel polygons, cycling when counts differ."""
    if count <= 0 or not panel_polygons:
        return []
    points: list[Any] = []
    usable = [poly for poly in panel_polygons if poly is not None and not getattr(poly, "is_empty", True)]
    if not usable:
        return []
    for i in range(count):
        poly = usable[i % len(usable)]
        try:
            pt = poly.representative_point()
        except Exception:
            try:
                pt = poly.centroid
            except Exception:
                continue
        points.append(pt)
    return points

def build_control_panel_polygons(
    instances: list[dict[str, Any]],
    cabins_gdf: gpd.GeoDataFrame | None,
    anchors: list[Any] | None = None,
    fixed_width_m: float | None = None,
    fixed_depth_m: float | None = None,
) -> list[Any]:
    """Build cabin-interior panel polygons arranged along the long side of each cabin."""
    if cabins_gdf is None or cabins_gdf.empty or not instances:
        return []

    fixed_w = None
    fixed_d = None
    if fixed_width_m is not None and fixed_depth_m is not None:
        try:
            fixed_w = max(float(fixed_width_m), 0.2)
            fixed_d = max(float(fixed_depth_m), 0.2)
        except Exception:
            fixed_w = None
            fixed_d = None

    def _fallback_layout() -> list[Any]:
        layout = _layout_points_in_cabins(cabins_gdf, len(instances), anchors)
        if not layout:
            return []
        if len(layout) < len(instances):
            last = layout[-1]
            while len(layout) < len(instances):
                layout.append(last)
        fallback_polys: list[Any] = []
        for inst, (center, cabin_poly) in zip(instances, layout):
            if fixed_w is not None and fixed_d is not None:
                width_m = fixed_w
                depth_m = fixed_d
            else:
                fields = inst.get("fields", {}) or {}
                width_val, width_key = _extract_length_from_fields(fields, ["width"])
                depth_val, depth_key = _extract_length_from_fields(fields, ["depth", "length"])
                width_m = _normalize_length_to_meters(width_val, width_key)
                depth_m = _normalize_length_to_meters(depth_val, depth_key)
                default_w, default_d = _default_panel_size_from_poly(cabin_poly)
                if width_m is None or width_m <= 0:
                    width_m = default_w
                if depth_m is None or depth_m <= 0:
                    depth_m = default_d
            try:
                if cabin_poly is not None:
                    minx, miny, maxx, maxy = cabin_poly.bounds
                    bbox_w = maxx - minx
                    bbox_h = maxy - miny
                    if bbox_w > 0:
                        width_m = min(width_m, bbox_w * 0.8)
                    if bbox_h > 0:
                        depth_m = min(depth_m, bbox_h * 0.8)
            except Exception:
                pass
            poly = _build_panel_rectangle(center, width_m, depth_m, cabin_poly)
            if poly is None and cabin_poly is not None:
                try:
                    poly = cabin_poly.centroid
                except Exception:
                    poly = None
            fallback_polys.append(poly)
        return fallback_polys

    cabin_polys = list(cabins_gdf.geometry)
    if not cabin_polys:
        return _fallback_layout()

    total = len(instances)
    n_cabins = len(cabin_polys)
    base = total // n_cabins
    remainder = total % n_cabins
    counts = [base + (1 if i < remainder else 0) for i in range(n_cabins)]

    panel_gap = 0.0
    min_size = 0.2
    out_polys: list[Any] = []
    inst_idx = 0
    for idx, cabin_poly in enumerate(cabin_polys):
        c = counts[idx]
        if c <= 0:
            continue
        inst_slice = instances[inst_idx : inst_idx + c]
        inst_idx += c

        coords = _get_polygon_coords(cabin_poly)
        if not coords:
            return _fallback_layout()
        ux, uy, cx, cy = _dominant_axis_from_coords(coords)
        px, py = -uy, ux

        alongs: list[float] = []
        perps: list[float] = []
        for x, y in coords:
            dx = x - cx
            dy = y - cy
            alongs.append(dx * ux + dy * uy)
            perps.append(-dx * uy + dy * ux)
        min_along = min(alongs)
        max_along = max(alongs)
        min_perp = min(perps)
        max_perp = max(perps)
        span_along = max_along - min_along
        span_perp = max_perp - min_perp
        if span_along <= 0 or span_perp <= 0:
            return _fallback_layout()

        margin_along = max(span_along * 0.05, 0.2)
        margin_perp = max(span_perp * 0.08, 0.2)
        usable_along = max(span_along - 2 * margin_along, min_size * c)
        usable_perp = max(span_perp - 2 * margin_perp, min_size)

        dims: list[tuple[float, float]] = []
        max_depth = 0.0
        for inst in inst_slice:
            if fixed_w is not None and fixed_d is not None:
                width_m = fixed_w
                depth_m = fixed_d
            else:
                fields = inst.get("fields", {}) or {}
                width_val, width_key = _extract_length_from_fields(fields, ["width"])
                depth_val, depth_key = _extract_length_from_fields(fields, ["depth", "length"])
                width_m = _normalize_length_to_meters(width_val, width_key)
                depth_m = _normalize_length_to_meters(depth_val, depth_key)
                default_w, default_d = _default_panel_size_from_poly(cabin_poly)
                if width_m is None or width_m <= 0:
                    width_m = default_w
                if depth_m is None or depth_m <= 0:
                    depth_m = default_d
            width_m = max(width_m, min_size)
            depth_m = max(depth_m, min_size)
            dims.append((width_m, depth_m))
            if depth_m > max_depth:
                max_depth = depth_m

        if usable_perp > 0 and max_depth > usable_perp * 0.8:
            scale_d = (usable_perp * 0.8) / max_depth
            dims = [(w, max(min_size, d * scale_d)) for w, d in dims]
            max_depth = max(d for _, d in dims) if dims else max_depth

        total_w = sum(w for w, _ in dims) + panel_gap * max(c - 1, 0)
        if total_w > usable_along and total_w > 0:
            scale_w = usable_along / total_w
            dims = [(max(min_size, w * scale_w), d) for w, d in dims]
            total_w = sum(w for w, _ in dims) + panel_gap * max(c - 1, 0)

        anchor = anchors[idx] if anchors and idx < len(anchors) else None
        anchor_perp = None
        if anchor is not None:
            try:
                adx = float(anchor.x) - cx
                ady = float(anchor.y) - cy
                anchor_perp = -adx * uy + ady * ux
            except Exception:
                anchor_perp = None

        if anchor_perp is not None:
            dist_to_max = abs(anchor_perp - max_perp)
            dist_to_min = abs(anchor_perp - min_perp)
            use_max_side = dist_to_max <= dist_to_min
        else:
            use_max_side = True

        if use_max_side:
            side_perp = max_perp - margin_perp
            inward_sign = -1.0
        else:
            side_perp = min_perp + margin_perp
            inward_sign = 1.0

        current_along = min_along + margin_along
        for width_m, depth_m in dims:
            center_along = current_along + width_m / 2.0
            center_perp = side_perp + inward_sign * (depth_m / 2.0)
            poly = _build_oriented_panel_rectangle(
                center_along,
                center_perp,
                width_m,
                depth_m,
                ux,
                uy,
                px,
                py,
                cx,
                cy,
                cabin_poly,
            )
            if poly is None:
                return _fallback_layout()
            out_polys.append(poly)
            current_along += width_m + panel_gap

    if len(out_polys) != len(instances):
        return _fallback_layout()
    return out_polys

def _get_polygon_coords(geom: Any) -> list[tuple[float, float]]:
    if geom is None or getattr(geom, "is_empty", True):
        return []
    try:
        geom_type = getattr(geom, "geom_type", "")
        if geom_type == "Polygon":
            return [(float(x), float(y)) for x, y in geom.exterior.coords]
        if geom_type == "MultiPolygon":
            poly = max(list(geom.geoms), key=lambda g: g.area, default=None)
            if poly is None:
                return []
            return [(float(x), float(y)) for x, y in poly.exterior.coords]
    except Exception:
        return []
    return []

def _dominant_axis_from_coords(coords: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    xs = [x for x, _ in coords]
    ys = [y for _, y in coords]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    dxs = [x - mean_x for x in xs]
    dys = [y - mean_y for y in ys]
    var_x = sum(d * d for d in dxs) / len(dxs)
    var_y = sum(d * d for d in dys) / len(dys)
    cov_xy = sum(dx * dy for dx, dy in zip(dxs, dys)) / len(dxs)
    if var_x + var_y < 1e-12:
        return 1.0, 0.0, mean_x, mean_y
    trace = var_x + var_y
    det = var_x * var_y - cov_xy * cov_xy
    disc = max(trace * trace / 4 - det, 0.0)
    lambda1 = trace / 2 + math.sqrt(disc)
    if abs(cov_xy) > 1e-12:
        vx = cov_xy
        vy = lambda1 - var_x
    else:
        if var_x >= var_y:
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0
    norm = math.hypot(vx, vy)
    if norm < 1e-12:
        vx, vy = (1.0, 0.0) if var_x >= var_y else (0.0, 1.0)
        norm = 1.0
    ux, uy = vx / norm, vy / norm
    if abs(uy) < 1e-9:
        if ux < 0:
            ux, uy = -ux, -uy
    elif uy < 0:
        ux, uy = -ux, -uy
    return ux, uy, mean_x, mean_y

def build_parallel_lines_for_polygon(geom: Any, count: int) -> list[Any]:
    """Create parallel lines that cross a polygon along its dominant axis."""
    if count <= 0:
        return []
    coords = _get_polygon_coords(geom)
    if not coords:
        return []
    try:
        from shapely.geometry import LineString
    except Exception:
        return []
    ux, uy, cx, cy = _dominant_axis_from_coords(coords)
    px, py = -uy, ux
    alongs: list[float] = []
    perps: list[float] = []
    for x, y in coords:
        dx = x - cx
        dy = y - cy
        alongs.append(dx * ux + dy * uy)
        perps.append(-dx * uy + dy * ux)
    min_along = min(alongs)
    max_along = max(alongs)
    min_perp = min(perps)
    max_perp = max(perps)
    length = max_along - min_along
    if length <= 0:
        return []
    margin = length * 0.05
    span = max_perp - min_perp
    if span <= 0:
        offsets = [min_perp] * count
    else:
        offsets = [min_perp + (i + 1) * span / (count + 1) for i in range(count)]
    lines: list[Any] = []
    for off in offsets:
        a0 = min_along - margin
        a1 = max_along + margin
        x0 = cx + a0 * ux + off * px
        y0 = cy + a0 * uy + off * py
        x1 = cx + a1 * ux + off * px
        y1 = cy + a1 * uy + off * py
        lines.append(LineString([(x0, y0), (x1, y1)]))
    return lines


@st.cache_data(show_spinner=False)

@st.cache_data(show_spinner=False)
def load_template_layer(path: Path) -> tuple[gpd.GeoDataFrame, str] | None:
    """Load the first layer from a template GeoPackage for geometry placement."""
    if path is None or not path.exists():
        return None
    layers = list_gpkg_layers(path)
    if not layers:
        return None
    layer = layers[0]
    try:
        gdf = gpd.read_file(path, layer=layer)
    except Exception:
        return None
    if gdf.empty or not hasattr(gdf, "geometry"):
        return None
    return gdf[[gdf.geometry.name]].copy(), layer

def expand_geometries(geoms: list[Any], target_count: int) -> list[Any]:
    """Expand or trim geometry list to match the target count (repeat if needed)."""
    if target_count <= 0 or not geoms:
        return []
    if len(geoms) >= target_count:
        return geoms[:target_count]
    expanded: list[Any] = []
    idx = 0
    while len(expanded) < target_count:
        expanded.append(geoms[idx % len(geoms)])
        idx += 1
    return expanded


@st.cache_data(show_spinner=False)

@st.cache_data(show_spinner=False)
def load_line_bay_layer(path: Path, layer: str | None, field: str | None) -> gpd.GeoDataFrame | None:
    """Load line bay polygons with the selected name field."""
    if path is None or layer is None:
        return None
    try:
        gdf = gpd.read_file(path, layer=layer)
    except Exception:
        return None
    if gdf.empty or not hasattr(gdf, "geometry"):
        return None
    if field is None or field not in gdf.columns:
        field = _pick_line_bay_name_field(gdf, field)
    if field is None or field not in gdf.columns:
        return None
    geom_col = gdf.geometry.name
    try:
        gdf = gdf[gdf[geom_col].notna() & ~gdf[geom_col].is_empty]
    except Exception:
        pass
    if gdf.empty:
        return None
    return gdf[[field, geom_col]].copy().reset_index(drop=True)

def collect_point_geometries_from_uploads(
    files: list[Any] | None,
    target_crs,
) -> gpd.GeoDataFrame | None:
    """Collect point geometries from uploaded GeoPackages."""
    if not files:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            gpkg_path = _coerce_gpkg_path(file_obj)
            if gpkg_path is None:
                continue
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                point_mask = geom_types.isin(["Point", "MultiPoint"])
                if not bool(point_mask.any()):
                    continue
                gdf_pts = gdf.loc[point_mask].copy()
                try:
                    if (gdf_pts.geometry.geom_type == "MultiPoint").any():
                        gdf_pts = gdf_pts.explode(index_parts=False)
                except Exception:
                    pass
                if target_crs is not None and gdf_pts.crs is not None and gdf_pts.crs != target_crs:
                    try:
                        gdf_pts = gdf_pts.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_pts.geometry, crs=target_crs or gdf_pts.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)

def collect_device_points_from_uploads(
    files: list[Any] | None,
    target_crs,
    device_options: list[str],
    equip_map: dict[str, str],
    target_device_norms: set[str],
) -> gpd.GeoDataFrame | None:
    """Collect point geometries from uploads for specific devices (e.g., Lightning Arrestor)."""
    if not files or not target_device_norms:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            file_name = _get_file_name(file_obj)
            dev_name = resolve_equipment_name(file_name, device_options, equip_map)
        except Exception:
            dev_name = None
        if normalize_for_compare(dev_name) not in target_device_norms:
            continue
        try:
            gpkg_path = _coerce_gpkg_path(file_obj)
            if gpkg_path is None:
                continue
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                point_mask = geom_types.isin(["Point", "MultiPoint"])
                if not bool(point_mask.any()):
                    continue
                gdf_pts = gdf.loc[point_mask].copy()
                try:
                    if (gdf_pts.geometry.geom_type == "MultiPoint").any():
                        gdf_pts = gdf_pts.explode(index_parts=False)
                except Exception:
                    pass
                if target_crs is not None and gdf_pts.crs is not None and gdf_pts.crs != target_crs:
                    try:
                        gdf_pts = gdf_pts.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_pts.geometry, crs=target_crs or gdf_pts.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)

def collect_device_polygons_from_uploads(
    files: list[Any] | None,
    target_crs,
    device_options: list[str],
    equip_map: dict[str, str],
    target_device_norms: set[str],
) -> gpd.GeoDataFrame | None:
    """Collect polygon geometries from uploads for specific devices (e.g., Cabins)."""
    if not files or not target_device_norms:
        return None
    frames: list[gpd.GeoDataFrame] = []
    cabin_norm = normalize_for_compare("Substation/Cabin")
    for file_obj in files:
        try:
            file_name = _get_file_name(file_obj)
            stem_norm = normalize_for_compare(Path(file_name).stem)
            dev_name = resolve_equipment_name(file_name, device_options, equip_map)
        except Exception:
            stem_norm = ""
            dev_name = None
        is_cabin_file = cabin_norm in target_device_norms and "cabin" in stem_norm
        if normalize_for_compare(dev_name) not in target_device_norms and not is_cabin_file:
            continue
        try:
            gpkg_path = _coerce_gpkg_path(file_obj)
            if gpkg_path is None:
                continue
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                poly_mask = geom_types.isin(["Polygon", "MultiPolygon"])
                if not bool(poly_mask.any()):
                    continue
                gdf_poly = gdf.loc[poly_mask].copy()
                if target_crs is not None and gdf_poly.crs is not None and gdf_poly.crs != target_crs:
                    try:
                        gdf_poly = gdf_poly.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_poly.geometry, crs=target_crs or gdf_poly.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)

def collect_device_linear_geometries_from_uploads(
    files: list[Any] | None,
    target_crs,
    device_options: list[str],
    equip_map: dict[str, str],
    target_device_norms: set[str],
) -> gpd.GeoDataFrame | None:
    """Collect line geometries from uploads for specific devices (e.g., High Voltage Line)."""
    if not files or not target_device_norms:
        return None
    frames: list[gpd.GeoDataFrame] = []
    for file_obj in files:
        try:
            file_name = _get_file_name(file_obj)
            dev_name = resolve_equipment_name(file_name, device_options, equip_map)
        except Exception:
            dev_name = None
        if normalize_for_compare(dev_name) not in target_device_norms:
            continue
        try:
            gpkg_path = _coerce_gpkg_path(file_obj)
            if gpkg_path is None:
                continue
            layers = list_gpkg_layers(gpkg_path)
            if not layers:
                continue
            for layer in layers:
                try:
                    gdf = gpd.read_file(gpkg_path, layer=layer)
                except Exception:
                    continue
                if gdf.empty or not hasattr(gdf, "geometry"):
                    continue
                geom_series = gdf.geometry
                try:
                    geom_types = geom_series.geom_type
                except Exception:
                    continue
                line_mask = geom_types.isin(["LineString", "MultiLineString"])
                if not bool(line_mask.any()):
                    continue
                gdf_lines = gdf.loc[line_mask].copy()
                try:
                    if (gdf_lines.geometry.geom_type == "MultiLineString").any():
                        gdf_lines = gdf_lines.explode(index_parts=False)
                except Exception:
                    pass
                if target_crs is not None and gdf_lines.crs is not None and gdf_lines.crs != target_crs:
                    try:
                        gdf_lines = gdf_lines.to_crs(target_crs)
                    except Exception:
                        pass
                frames.append(gpd.GeoDataFrame(geometry=gdf_lines.geometry, crs=target_crs or gdf_lines.crs))
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)

def map_points_to_bays(
    points_gdf: gpd.GeoDataFrame | None,
    bay_gdf: gpd.GeoDataFrame,
) -> dict[int, list[Any]]:
    """Map points to line bay polygon indices (intersects or near-touch, ordered shallowest points first)."""
    if points_gdf is None or points_gdf.empty:
        return {}
    if points_gdf.crs is not None and bay_gdf.crs is not None and points_gdf.crs != bay_gdf.crs:
        try:
            points_gdf = points_gdf.to_crs(bay_gdf.crs)
        except Exception:
            pass
    joined = None
    try:
        joined = gpd.sjoin(points_gdf, bay_gdf, how="left", predicate="intersects")
    except TypeError:
        try:
            joined = gpd.sjoin(points_gdf, bay_gdf, how="left", op="intersects")
        except Exception:
            joined = None
    except Exception:
        joined = None
    if joined is None or "index_right" not in joined.columns:
        out: dict[int, list[Any]] = {}
        try:
            bay_items = list(bay_gdf.geometry.items())
        except Exception:
            bay_items = []
        for pt in points_gdf.geometry:
            if pt is None or getattr(pt, "is_empty", True):
                continue
            for idx, poly in bay_items:
                if poly is None or getattr(poly, "is_empty", True):
                    continue
                try:
                    if poly.intersects(pt):
                        try:
                            key = int(idx)
                        except Exception:
                            key = idx
                        out.setdefault(key, []).append(pt)
                        break
                except Exception:
                    continue
        return out
    out: dict[int, list[Any]] = {}
    used_point_idx: set[int] = set()
    if joined is not None:
        for idx, row in joined.iterrows():
            bay_idx = row.get("index_right")
            if pd.isna(bay_idx):
                continue
            try:
                bay_key = int(bay_idx)
            except Exception:
                continue
            out.setdefault(bay_key, []).append((idx, row.geometry))
            used_point_idx.add(idx)

    bay_geoms: list[tuple[int, Any, float]] = []
    try:
        for idx, geom in bay_gdf.geometry.items():
            if geom is None or getattr(geom, "is_empty", True):
                continue
            try:
                width = geom.bounds[2] - geom.bounds[0]
                height = geom.bounds[3] - geom.bounds[1]
                min_dim = min(width, height)
            except Exception:
                min_dim = 0.0
            tol = max(0.1, min_dim * 0.15)
            bay_geoms.append((idx, geom, tol))
    except Exception:
        bay_geoms = []

    if bay_geoms:
        for idx_pt, pt in enumerate(points_gdf.geometry):
            if idx_pt in used_point_idx:
                continue
            if pt is None or getattr(pt, "is_empty", True):
                continue
            best_idx = None
            best_dist = None
            for bay_idx, bay_geom, tol in bay_geoms:
                try:
                    dist = bay_geom.distance(pt)
                except Exception:
                    continue
                if dist <= tol and (best_dist is None or dist < best_dist):
                    best_dist = dist
                    best_idx = bay_idx
            if best_idx is not None:
                try:
                    bay_key = int(best_idx)
                except Exception:
                    bay_key = best_idx
                out.setdefault(bay_key, []).append((idx_pt, pt))

    ordered_out: dict[int, list[Any]] = {}
    for bay_idx, items in out.items():
        sorted_items = sorted(items, key=lambda t: t[0])
        ordered_out[bay_idx] = [geom for _, geom in sorted_items]
    return ordered_out

def _pick_line_bay_name_field(df: gpd.GeoDataFrame, selected: str | None) -> str | None:
    """Choose a name-bearing column from a Line Bay layer, preferring *_name over *_id."""
    if df is None or df.empty:
        return selected
    cols = list(df.columns)
    if hasattr(df, "geometry") and df.geometry.name in cols:
        cols = [c for c in cols if c != df.geometry.name]
    if not cols:
        return selected

    def _score(col: str) -> int:
        norm = normalize_for_compare(col)
        score = 0
        if "name" in norm:
            score += 5
        if "line" in norm:
            score += 2
        if "bay" in norm:
            score += 2
        if "id" in norm:
            score -= 3
        return score

    lookup = {normalize_for_compare(c): c for c in cols}
    sel_norm = normalize_for_compare(selected) if selected else ""
    best_col = None
    best_score = -999
    for c in cols:
        sc = _score(c)
        if sel_norm and normalize_for_compare(c) == sel_norm:
            sc += 1  # slight bias to user's pick
        if sc > best_score:
            best_score = sc
            best_col = c
    if best_col:
        return best_col
    if sel_norm and sel_norm in lookup:
        return lookup[sel_norm]
    return cols[0]

def _build_line_bay_id_name_map(workbook_path: Path | None, sheet_name: str | None) -> dict[str, Any]:
    """Build mapping of Line_Bay_ID -> Line_Bay_Name from the supervisor sheet."""
    if workbook_path is None or sheet_name is None:
        return {}
    try:
        instances = parse_supervisor_device_table(workbook_path, sheet_name, "Line Bay")
    except Exception:
        return {}
    mapping: dict[str, Any] = {}
    for inst in instances:
        fields = inst.get("fields", {}) or {}
        lookup = {normalize_for_compare(k): k for k in fields.keys()}
        id_val = None
        name_val = None
        for alias in ["line_bay_id", "linebayid", "line bay id", "line_bayid", "line bay_id"]:
            key = lookup.get(normalize_for_compare(alias))
            if key:
                id_val = fields.get(key)
                break
        for alias in ["line_bay_name", "linebayname", "line bay name", "line_bayname", "name"]:
            key = lookup.get(normalize_for_compare(alias))
            if key:
                name_val = fields.get(key)
                break
        norm_id = normalize_value_for_compare(id_val)
        if norm_id and name_val is not None and pd.notna(name_val):
            mapping[norm_id] = name_val
    return mapping

def _collect_geom_points(geom: Any) -> list[Any]:
    """Flatten a geometry into point members when available."""
    if geom is None or getattr(geom, "is_empty", True):
        return []
    geom_type = getattr(geom, "geom_type", "")
    if geom_type == "Point":
        return [geom]
    if geom_type == "MultiPoint":
        try:
            return [part for part in geom.geoms if part is not None and not getattr(part, "is_empty", True)]
        except Exception:
            return []
    points: list[Any] = []
    try:
        for part in geom.geoms:
            points.extend(_collect_geom_points(part))
    except Exception:
        return []
    return points

def _build_line_exit_reference_point(line_geom: Any, bay_geom: Any, bay_center: Any) -> Any | None:
    """Pick a boundary-side reference point for a line entering or leaving a bay."""
    if (
        line_geom is None
        or getattr(line_geom, "is_empty", True)
        or bay_geom is None
        or getattr(bay_geom, "is_empty", True)
    ):
        return None
    try:
        from shapely.geometry import Point
        from shapely.ops import nearest_points
    except Exception:
        return None

    center = bay_center
    if center is None or getattr(center, "is_empty", True):
        try:
            center = bay_geom.centroid
        except Exception:
            center = None

    segments = [line_geom]
    try:
        if getattr(line_geom, "geom_type", "") == "MultiLineString":
            segments = [seg for seg in line_geom.geoms if seg is not None and not getattr(seg, "is_empty", True)]
    except Exception:
        segments = [line_geom]

    boundary = None
    try:
        boundary = bay_geom.boundary
    except Exception:
        boundary = None

    best_pt = None
    best_score = None
    for seg in segments:
        try:
            coords = list(seg.coords)
        except Exception:
            continue
        if len(coords) < 2:
            continue
        endpoints = [Point(coords[0]), Point(coords[-1])]
        if center is not None and not getattr(center, "is_empty", True):
            outer_endpoint = max(endpoints, key=lambda pt: float(pt.distance(center)))
        else:
            outer_endpoint = endpoints[0]

        candidate = None
        if boundary is not None:
            try:
                boundary_hits = _collect_geom_points(seg.intersection(boundary))
            except Exception:
                boundary_hits = []
            if boundary_hits:
                candidate = min(boundary_hits, key=lambda pt: float(pt.distance(outer_endpoint)))
        if candidate is None:
            candidate = outer_endpoint
            if boundary is not None:
                try:
                    candidate = nearest_points(candidate, boundary)[1]
                except Exception:
                    pass
        try:
            score = float(candidate.distance(outer_endpoint))
        except Exception:
            score = 0.0
        if best_score is None or score < best_score:
            best_pt = candidate
            best_score = score
    return best_pt

def enrich_line_bay_reference_info(
    files: list[Any] | None,
    device_options: list[str],
    equip_map: dict[str, str],
    line_bay_info: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Populate per-bay centroids and reference points used by post-fill device placement."""
    if not isinstance(line_bay_info, dict):
        return line_bay_info
    bay_ref_gdf = load_line_bay_layer(
        line_bay_info.get("path"),
        line_bay_info.get("layer"),
        line_bay_info.get("field"),
    )
    if bay_ref_gdf is None or bay_ref_gdf.empty:
        return line_bay_info

    geom_col_ref = bay_ref_gdf.geometry.name if hasattr(bay_ref_gdf, "geometry") else None
    bay_val_cols = [c for c in bay_ref_gdf.columns if c != geom_col_ref]
    bay_val_col = bay_val_cols[0] if bay_val_cols else None

    id_name_map = line_bay_info.get("id_name_map") if isinstance(line_bay_info, dict) else {}
    reverse_name_to_id: dict[str, str] = {}
    if isinstance(id_name_map, dict):
        for k, v in id_name_map.items():
            k_norm = normalize_value_for_compare(k)
            v_norm = normalize_value_for_compare(v)
            if k_norm and v_norm and v_norm not in reverse_name_to_id:
                reverse_name_to_id[v_norm] = k_norm

    def _canon_bay_key(raw_val: Any) -> str:
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

    bay_centroid_by_key: dict[str, Any] = {}
    bay_geom_by_idx: dict[int, Any] = {}
    bay_center_by_idx: dict[int, Any] = {}
    if bay_val_col and geom_col_ref:
        for idx, row in bay_ref_gdf.iterrows():
            key = _canon_bay_key(row.get(bay_val_col))
            geom = row.get(geom_col_ref)
            if geom is None or getattr(geom, "is_empty", True):
                continue
            try:
                idx_key = int(idx)
            except Exception:
                idx_key = idx
            bay_geom_by_idx[idx_key] = geom
            try:
                center = geom.centroid
            except Exception:
                try:
                    center = geom.representative_point()
                except Exception:
                    center = None
            if center is not None and not getattr(center, "is_empty", True):
                bay_center_by_idx[idx_key] = center
                if key:
                    bay_centroid_by_key[key] = center
    line_bay_info["bay_centroid_by_key"] = bay_centroid_by_key

    def _group_points_by_key(points_gdf: gpd.GeoDataFrame | None) -> dict[str, Any]:
        refs_by_key: dict[str, Any] = {}
        if points_gdf is None or points_gdf.empty or not bay_val_col:
            return refs_by_key
        points_by_bay = map_points_to_bays(points_gdf, bay_ref_gdf)
        grouped_pts: dict[str, list[Any]] = {}
        for bay_idx, pts in points_by_bay.items():
            if not pts:
                continue
            try:
                bay_row = bay_ref_gdf.iloc[int(bay_idx)]
            except Exception:
                continue
            key = _canon_bay_key(bay_row.get(bay_val_col))
            if not key:
                continue
            grouped_pts.setdefault(key, []).extend(pts)
        for key, pts in grouped_pts.items():
            valid_pts = [p for p in pts if p is not None and not getattr(p, "is_empty", True)]
            if not valid_pts:
                continue
            if len(valid_pts) == 1:
                refs_by_key[key] = valid_pts[0]
                continue
            try:
                from shapely.geometry import MultiPoint

                refs_by_key[key] = MultiPoint(valid_pts).centroid
            except Exception:
                refs_by_key[key] = valid_pts[0]
        return refs_by_key

    vt_norms = {normalize_for_compare("Voltage Transformer")}
    vt_points = collect_device_points_from_uploads(
        files,
        bay_ref_gdf.crs,
        device_options,
        equip_map,
        vt_norms,
    )
    line_bay_info["vt_ref_by_key"] = _group_points_by_key(vt_points)

    hv_line_norms = {normalize_for_compare("High Voltage Line")}
    hv_lines = collect_device_linear_geometries_from_uploads(
        files,
        bay_ref_gdf.crs,
        device_options,
        equip_map,
        hv_line_norms,
    )
    exit_points: list[Any] = []
    if hv_lines is not None and not hv_lines.empty:
        for line_geom in hv_lines.geometry:
            if line_geom is None or getattr(line_geom, "is_empty", True):
                continue
            for bay_idx, bay_geom in bay_geom_by_idx.items():
                if bay_geom is None or getattr(bay_geom, "is_empty", True):
                    continue
                try:
                    if not line_geom.intersects(bay_geom):
                        continue
                except Exception:
                    continue
                ref_pt = _build_line_exit_reference_point(
                    line_geom,
                    bay_geom,
                    bay_center_by_idx.get(bay_idx),
                )
                if ref_pt is not None and not getattr(ref_pt, "is_empty", True):
                    exit_points.append(ref_pt)
    exit_points_gdf = (
        gpd.GeoDataFrame(geometry=exit_points, crs=bay_ref_gdf.crs) if exit_points else None
    )
    line_bay_info["line_exit_ref_by_key"] = _group_points_by_key(exit_points_gdf)

    busbar_norms = {normalize_for_compare("High Voltage Busbar/Medium Voltage Busbar")}
    busbar_lines = collect_device_linear_geometries_from_uploads(
        files,
        bay_ref_gdf.crs,
        device_options,
        equip_map,
        busbar_norms,
    )
    if busbar_lines is not None and not busbar_lines.empty:
        line_bay_info["busbar_geometries"] = [
            geom
            for geom in busbar_lines.geometry
            if geom is not None and not getattr(geom, "is_empty", True)
        ]
    else:
        line_bay_info["busbar_geometries"] = []

    return line_bay_info

def _extract_bay_name_from_row(row: pd.Series, name_field: str | None, id_name_map: dict[str, Any]) -> Any:
    """Resolve a bay name from a row, falling back to id->name map and other name-like columns."""
    bay_val = row.get(name_field) if name_field else None
    lookup = {normalize_for_compare(k): k for k in row.index}

    def _get_by_alias(aliases: list[str]) -> Any:
        for alias in aliases:
            key = lookup.get(normalize_for_compare(alias))
            if key:
                return row.get(key)
        return None

    id_val = _get_by_alias(["line_bay_id", "linebayid", "line bay id", "line_bayid", "line bay_id"])
    norm_id = normalize_value_for_compare(id_val if id_val is not None else bay_val)
    if norm_id and norm_id in id_name_map:
        return id_name_map[norm_id]

    if bay_val is None or pd.isna(bay_val):
        name_alt = _get_by_alias(["line_bay_name", "linebayname", "line bay name", "line_bayname", "name"])
        if name_alt is not None and not pd.isna(name_alt):
            return name_alt
    return bay_val

def replace_line_name_ids(out_gdf: gpd.GeoDataFrame, id_name_map: dict[str, Any], name_fields: list[str] | None = None) -> gpd.GeoDataFrame:
    """Replace Name/Line name columns that still contain Line_Bay_ID with the corresponding Line_Bay_Name."""
    if out_gdf is None or out_gdf.empty or not isinstance(id_name_map, dict) or not id_name_map:
        return out_gdf
    name_fields = name_fields or [
        "Name",
        "name",
        "Line_Name",
        "line_name",
        "line",
        "Line",
        "Line_Bay_Name",
        "line_bay_name",
    ]
    id_lookup = {normalize_value_for_compare(k): v for k, v in id_name_map.items()}
    out = out_gdf.copy()
    for col in name_fields:
        if col not in out.columns:
            continue
        try:
            series = out[col]
            mapped = series.map(lambda v: id_lookup.get(normalize_value_for_compare(v), v) if pd.notna(v) else v)
            out[col] = mapped
        except Exception:
            continue
    return out

def apply_line_bay_names(out_gdf: gpd.GeoDataFrame, line_bay_info: dict[str, Any], geom_name: str) -> gpd.GeoDataFrame:
    """Assign line name fields based on intersecting/nearest Line Bay polygons."""
    if out_gdf is None or out_gdf.empty or geom_name not in out_gdf.columns:
        return out_gdf
    if line_bay_info.get("path") and line_bay_info.get("layer") is None:
        try:
            import fiona

            layers = fiona.listlayers(line_bay_info.get("path"))
            if layers:
                line_bay_info = dict(line_bay_info)
                line_bay_info["layer"] = layers[0]
        except Exception:
            pass
    bay_gdf = load_line_bay_layer(
        line_bay_info.get("path"),
        line_bay_info.get("layer"),
        line_bay_info.get("field"),
    )
    if bay_gdf is None or bay_gdf.empty:
        return out_gdf
    bay_field = _pick_line_bay_name_field(bay_gdf, line_bay_info.get("field"))
    id_name_map = line_bay_info.get("id_name_map") if isinstance(line_bay_info, dict) else {}
    if not isinstance(id_name_map, dict):
        id_name_map = {}
    try:
        if out_gdf.crs is not None and bay_gdf.crs is not None and out_gdf.crs != bay_gdf.crs:
            bay_gdf = bay_gdf.to_crs(out_gdf.crs)
    except Exception:
        pass

    name_fields = [
        "Name",
        "name",
        "Line_Name",
        "line_name",
        "line",
        "Line",
        "Line_Bay_Name",
        "line_bay_name",
    ]

    # Build an id->name map directly from the Line Bay layer to support attribute-based replacement.
    layer_id_map: dict[str, Any] = {}
    id_aliases = ["line_bay_id", "linebayid", "line bay id", "line_bayid", "line bay_id"]
    for _, row in bay_gdf.iterrows():
        lookup = {normalize_for_compare(k): k for k in row.index}
        id_val = None
        for alias in id_aliases:
            key = lookup.get(normalize_for_compare(alias))
            if key:
                id_val = row.get(key)
                break
        name_val = _extract_bay_name_from_row(row, bay_field, id_name_map)
        norm_id = normalize_value_for_compare(id_val)
        if norm_id and name_val is not None and not pd.isna(name_val):
            layer_id_map.setdefault(norm_id, name_val)

    combined_id_map = dict(id_name_map)
    for k, v in layer_id_map.items():
        combined_id_map.setdefault(k, v)

    bay_lookup: dict[int, Any] = {}
    try:
        joined = gpd.sjoin(out_gdf[[geom_name]].set_geometry(geom_name), bay_gdf, how="left", predicate="intersects")
    except TypeError:
        try:
            joined = gpd.sjoin(out_gdf[[geom_name]].set_geometry(geom_name), bay_gdf, how="left", op="intersects")
        except Exception:
            joined = None
    except Exception:
        joined = None
    if joined is not None and "index_right" in joined.columns:
        for idx, row in joined.iterrows():
            bay_idx = row.get("index_right")
            if pd.isna(bay_idx):
                continue
            try:
                bay_row = bay_gdf.iloc[int(bay_idx)]
                bay_name_val = _extract_bay_name_from_row(bay_row, bay_field, id_name_map)
            except Exception:
                bay_name_val = None
            if bay_name_val is not None:
                bay_lookup[idx] = bay_name_val

    # Nearest-bay fallback for any lines without match
    if len(bay_lookup) < len(out_gdf):
        try:
            bay_centroids = [(idx, geom.centroid) for idx, geom in bay_gdf.geometry.items() if geom is not None and not geom.is_empty]
            for idx, geom in out_gdf.geometry.items():
                if idx in bay_lookup:
                    continue
                if geom is None or getattr(geom, "is_empty", True):
                    continue
                line_centroid = geom.centroid
                best_idx = None
                best_dist = None
                for b_idx, b_cent in bay_centroids:
                    try:
                        dist = line_centroid.distance(b_cent)
                    except Exception:
                        continue
                    if best_dist is None or dist < best_dist:
                        best_dist = dist
                        best_idx = b_idx
                if best_idx is not None:
                    try:
                        bay_row = bay_gdf.iloc[int(best_idx)]
                        bay_name_val = _extract_bay_name_from_row(bay_row, bay_field, id_name_map)
                    except Exception:
                        bay_name_val = None
                    if bay_name_val is not None:
                        bay_lookup[idx] = bay_name_val
        except Exception:
            pass

    if bay_lookup:
        target_cols = [c for c in name_fields if c in out_gdf.columns]
        if not target_cols:
            target_cols = ["Name"]
            if "Name" not in out_gdf.columns:
                out_gdf["Name"] = pd.NA
        for idx, bay_name_val in bay_lookup.items():
            for col in target_cols:
                try:
                    out_gdf.loc[idx, col] = bay_name_val
                except Exception:
                    continue
        # ensure name fields are strings to avoid schema errors on write
        for col in target_cols:
            try:
                out_gdf[col] = out_gdf[col].astype("string")
            except Exception:
                try:
                    out_gdf[col] = out_gdf[col].astype(str)
                except Exception:
                    pass

    # Final pass: replace any remaining Line_Bay_ID values with names using combined map.
    if combined_id_map:
        out_gdf = replace_line_name_ids(out_gdf, combined_id_map, name_fields=name_fields)
    return out_gdf

def group_points_by_perp_gap(
    items: list[tuple[Any, float, float]],
    group_count: int,
) -> list[list[tuple[Any, float, float]]]:
    """Group items by closest gaps along perpendicular coordinate."""
    if not items or group_count <= 0:
        return []
    group_count = min(group_count, len(items))
    items_sorted = sorted(items, key=lambda t: (t[2], t[1]))
    groups: list[list[tuple[Any, float, float]]] = [[item] for item in items_sorted]
    while len(groups) > group_count:
        gaps = [
            groups[i + 1][0][2] - groups[i][-1][2]
            for i in range(len(groups) - 1)
        ]
        merge_idx = gaps.index(min(gaps))
        groups[merge_idx].extend(groups[merge_idx + 1])
        del groups[merge_idx + 1]
    return groups

def build_lines_from_points_in_polygon(
    polygon: Any,
    points: list[Any],
    count: int,
) -> list[Any]:
    """Build line strings for a polygon using internal points, fallback to parallel lines."""
    if count <= 0:
        return []
    coords = _get_polygon_coords(polygon)
    if not coords:
        return []
    if not points:
        return build_parallel_lines_for_polygon(polygon, count)
    try:
        from shapely.geometry import LineString, Point
    except Exception:
        return []
    ux, uy, cx, cy = _dominant_axis_from_coords(coords)
    alongs: list[float] = []
    for x, y in coords:
        dx = x - cx
        dy = y - cy
        alongs.append(dx * ux + dy * uy)
    if not alongs:
        return []
    min_along = min(alongs)
    max_along = max(alongs)
    items: list[tuple[Any, float, float]] = []
    for pt in points:
        if pt is None or getattr(pt, "is_empty", True):
            continue
        try:
            p = pt if getattr(pt, "geom_type", "") == "Point" else pt.centroid
            x = float(p.x)
            y = float(p.y)
        except Exception:
            continue
        dx = x - cx
        dy = y - cy
        along = dx * ux + dy * uy
        perp = -dx * uy + dy * ux
        items.append((p, along, perp))
    if len(items) < count:
        return build_parallel_lines_for_polygon(polygon, count)
    groups = group_points_by_perp_gap(items, count)
    if len(groups) < count:
        return build_parallel_lines_for_polygon(polygon, count)
    margin = (max_along - min_along) * 0.05
    px, py = -uy, ux
    lines: list[Any] = []
    for group in groups:
        group_sorted = sorted(group, key=lambda t: t[1])
        if not group_sorted:
            continue
        if len(group_sorted) > 2:
            group_sorted = [group_sorted[0], group_sorted[-1]]
        avg_perp = sum(item[2] for item in group_sorted) / len(group_sorted)
        group_mean_along = sum(item[1] for item in group_sorted) / len(group_sorted)
        dist_to_min = abs(group_mean_along - min_along)
        dist_to_max = abs(max_along - group_mean_along)
        extend_min_side = dist_to_min <= dist_to_max
        if not extend_min_side:
            group_sorted = list(reversed(group_sorted))
        extend_along = (min_along - margin * 2) if extend_min_side else (max_along + margin * 2)
        start_pt = Point(cx + extend_along * ux + avg_perp * px, cy + extend_along * uy + avg_perp * py)
        path = [start_pt] + [item[0] for item in group_sorted]
        lines.append(LineString([(p.x, p.y) for p in path]))
    if len(lines) != count:
        return build_parallel_lines_for_polygon(polygon, count)
    return lines


_NUM_REGEX = re.compile(r"[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?".replace("\\\\", "\\"))

def _extract_first_number(value: Any) -> float | None:
    """Extract the first numeric value from a string; returns None if none found."""
    if pd.isna(value):
        return None
    text = str(value)
    # Normalize minus signs/spaces
    text = text.replace("\u2212", "-")
    text = text.replace("−", "-")
    m = _NUM_REGEX.search(text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

