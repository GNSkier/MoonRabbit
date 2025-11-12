#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Iterable

import json
import pandas as pd


def _safe_to_float(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series to float with coercion and return the cleaned series.
    """
    converted = pd.to_numeric(series, errors="coerce")
    return converted


def _build_state_to_coords_from_df(
    df: pd.DataFrame, state_col: str, lon_col: str, lat_col: str
) -> Dict[str, List[Tuple[float, float]]]:
    """
    Build state -> list[(lon, lat)] dictionary from given dataframe using provided column names.
    """
    result: Dict[str, List[Tuple[float, float]]] = {}

    # Ensure numeric and drop missing coordinates
    df = df.copy()
    df[lon_col] = _safe_to_float(df[lon_col])
    df[lat_col] = _safe_to_float(df[lat_col])
    df = df.dropna(subset=[lon_col, lat_col, state_col])

    # Group by state and collect tuples
    for state, group in df.groupby(state_col):
        # Preserve insertion order; avoid trivial duplicates
        seen = set()
        coords: List[Tuple[float, float]] = []
        for lon, lat in zip(
            group[lon_col].astype(float), group[lat_col].astype(float)
        ):
            key = (lon, lat)
            if key in seen:
                continue
            seen.add(key)
            coords.append(key)
        result[str(state)] = coords
    return result


def _merge_state_coord_dicts(
    base: Dict[str, List[Tuple[float, float]]],
    additional: Dict[str, List[Tuple[float, float]]],
) -> Dict[str, List[Tuple[float, float]]]:
    """
    Merge two state->coords dicts, preserving order and de-duplicating coordinates per state.
    """
    merged: Dict[str, List[Tuple[float, float]]] = {
        k: list(v) for k, v in base.items()
    }
    for state, coords in additional.items():
        if state not in merged:
            merged[state] = list(coords)
            continue
        seen = set(merged[state])
        for coord in coords:
            if coord not in seen:
                merged[state].append(coord)
                seen.add(coord)
    return merged


def _detect_lat_lon_columns(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    """
    Attempt to detect longitude and latitude column names in a CSV.
    Returns (lon_col, lat_col) if found, else None.
    """
    lower_cols = {c.lower(): c for c in df.columns}
    # Common patterns
    candidates = [
        ("longitude", "latitude"),
        ("lon", "lat"),
        ("long", "lat"),
        ("intptlong", "intptlat"),
    ]
    for lon_key, lat_key in candidates:
        if lon_key in lower_cols and lat_key in lower_cols:
            return lower_cols[lon_key], lower_cols[lat_key]
    return None


def extract_state_coordinates(
    gazetteer_path: str | Path,
    csv_path: Optional[str | Path] = None,
    allowed_states: Optional[Iterable[str]] = None,
) -> Dict[str, List[Tuple[float, float]]]:
    """
    Extract all (longitude, latitude) coordinates by state abbreviation from:
    - US Census counties gazetteer .txt file (tab-delimited)
    - Optional CSV that contains columns for state and lon/lat

    Returns a dictionary: { 'STATE_ABBR': [(lon, lat), ...], ... }
    """
    # Read gazetteer
    gazetteer_path = Path(gazetteer_path)
    if not gazetteer_path.exists():
        raise FileNotFoundError(f"Gazetteer file not found: {gazetteer_path}")

    df_gaz = pd.read_csv(gazetteer_path, sep="\t", dtype=str, encoding="latin1")
    df_gaz.columns = df_gaz.columns.str.strip()

    required_cols = {"USPS", "INTPTLONG", "INTPTLAT"}
    missing = required_cols - set(df_gaz.columns)
    if missing:
        raise ValueError(f"Missing required columns in gazetteer: {missing}")

    # If a state filter is provided, restrict to only those states first
    if allowed_states is not None:
        allowed_set = {str(s) for s in allowed_states}
        if "USPS" in df_gaz.columns:
            df_gaz = df_gaz[df_gaz["USPS"].isin(allowed_set)]

    state_to_coords = _build_state_to_coords_from_df(
        df=df_gaz, state_col="USPS", lon_col="INTPTLONG", lat_col="INTPTLAT"
    )

    # Optionally merge CSV coordinates
    if csv_path is not None:
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        df_csv = pd.read_csv(csv_path)
        df_csv.columns = df_csv.columns.str.strip()

        # Identify state column
        state_col: Optional[str] = None
        for cand in ["State", "USPS", "state", "usps"]:
            if cand in df_csv.columns:
                state_col = cand
                break
        if state_col is None:
            # If there's no state column, we cannot incorporate this CSV
            pass
        else:
            # If filtering, apply to the CSV as well
            if allowed_states is not None:
                allowed_set = {str(s) for s in allowed_states}
                df_csv = df_csv[df_csv[state_col].isin(allowed_set)]

            detected = _detect_lat_lon_columns(df_csv)
            if detected is not None:
                lon_col, lat_col = detected
                additional = _build_state_to_coords_from_df(
                    df=df_csv,
                    state_col=state_col,
                    lon_col=lon_col,
                    lat_col=lat_col,
                )
                state_to_coords = _merge_state_coord_dicts(
                    state_to_coords, additional
                )

    return state_to_coords


def _default_paths() -> Tuple[Path, Optional[Path]]:
    """
    Compute default paths relative to this file for convenience.
    """
    base_dir = Path(__file__).parent
    gazetteer = base_dir / "data" / "2024_Gaz_counties_national.txt"
    # Known CSV with lat/lon in this repo
    soybean_csv = base_dir / "soybean_counties_coordinates.csv"
    return gazetteer, soybean_csv if soybean_csv.exists() else None


def main(argv: List[str]) -> int:
    """
    Simple CLI: prints count of coordinates per state.
    """
    if len(argv) >= 2:
        gaz_path = Path(argv[1])
    else:
        gaz_path, _ = _default_paths()

    if len(argv) >= 3:
        csv_path: Optional[Path] = Path(argv[2])
    else:
        _, csv_path = _default_paths()

    # Optional output path (3rd argument)
    if len(argv) >= 4:
        output_path = Path(argv[3])
    else:
        output_path = Path(__file__).parent / "state_coordinates.txt"

    # Attempt to use the curated state filter from utilities.py if available
    allowed_states: Optional[List[str]] = None
    try:
        # Try both when run from repo root or other working dirs
        from utilities import weather_station_states as _allowed

        allowed_states = list(_allowed)
    except Exception:
        try:
            # If running with a different PYTHONPATH, try to resolve parent
            sys.path.append(str(Path(__file__).resolve().parents[1]))
            from utilities import weather_station_states as _allowed  # type: ignore

            allowed_states = list(_allowed)
        except Exception:
            allowed_states = None

    state_to_coords = extract_state_coordinates(
        gaz_path, csv_path, allowed_states
    )

    print(f"Total states: {len(state_to_coords)}")
    total_points = sum(len(v) for v in state_to_coords.values())
    print(f"Total coordinate points: {total_points}")
    print("Counts by state (sorted):")
    for state in sorted(state_to_coords.keys()):
        print(f"{state}: {len(state_to_coords[state])}")

    # Write dictionary to txt (JSON format)
    try:
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(state_to_coords, f, ensure_ascii=False, indent=2)
        print(f"\nSaved coordinates to: {output_path}")
    except Exception as e:
        print(f"\nERROR: Failed to write output to {output_path}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
