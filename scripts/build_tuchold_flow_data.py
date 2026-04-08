from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_SOURCE = Path("detention-stays_filtered_20260407_075932.parquet")
DEFAULT_FACILITY_SOURCE = Path("data/facility_coordinates.parquet")
DEFAULT_OUTPUT_JSON = Path("web/data/tuchold_flow_data.json")
DEFAULT_OUTPUT_JS = Path("web/data/tuchold_flow_data.js")
DEFAULT_YEARS = [2025, 2026]
DEFAULT_FIRST_FACILITY_CODE = "TUCHOLD"
REQUIRED_FACILITY_COLUMNS = {"facility_code", "name", "short_label", "city", "state", "lat", "lon"}


COUNTRY_NODES: dict[str, dict[str, Any]] = {
    "AUSTRALIA": {"short_label": "Australia", "lat": -35.2809, "lon": 149.1300},
    "BOLIVIA": {"short_label": "Bolivia", "lat": -16.4897, "lon": -68.1193},
    "BRAZIL": {"short_label": "Brazil", "lat": -15.7939, "lon": -47.8828},
    "CHILE": {"short_label": "Chile", "lat": -33.4489, "lon": -70.6693},
    "CHINA, PEOPLES REPUBLIC OF": {"short_label": "China", "lat": 39.9042, "lon": 116.4074},
    "COLOMBIA": {"short_label": "Colombia", "lat": 4.7110, "lon": -74.0721},
    "COSTA RICA": {"short_label": "Costa Rica", "lat": 9.9281, "lon": -84.0907},
    "DOMINICAN REPUBLIC": {"short_label": "Dominican Rep.", "lat": 18.4861, "lon": -69.9312},
    "ECUADOR": {"short_label": "Ecuador", "lat": -0.1807, "lon": -78.4678},
    "EL SALVADOR": {"short_label": "El Salvador", "lat": 13.6929, "lon": -89.2182},
    "GERMANY": {"short_label": "Germany", "lat": 52.5200, "lon": 13.4050},
    "GUATEMALA": {"short_label": "Guatemala", "lat": 14.6349, "lon": -90.5069},
    "HONDURAS": {"short_label": "Honduras", "lat": 14.0723, "lon": -87.1921},
    "IRAQ": {"short_label": "Iraq", "lat": 33.3152, "lon": 44.3661},
    "ITALY": {"short_label": "Italy", "lat": 41.9028, "lon": 12.4964},
    "JAMAICA": {"short_label": "Jamaica", "lat": 18.0179, "lon": -76.8099},
    "JORDAN": {"short_label": "Jordan", "lat": 31.9539, "lon": 35.9106},
    "KENYA": {"short_label": "Kenya", "lat": -1.2921, "lon": 36.8219},
    "LAOS": {"short_label": "Laos", "lat": 17.9757, "lon": 102.6331},
    "LIBERIA": {"short_label": "Liberia", "lat": 6.3004, "lon": -10.7969},
    "MALI": {"short_label": "Mali", "lat": 12.6392, "lon": -8.0029},
    "MEXICO": {"short_label": "Mexico", "lat": 19.4326, "lon": -99.1332},
    "MICRONESIA, FEDERATED STATES OF": {"short_label": "Micronesia", "lat": 6.9248, "lon": 158.1610},
    "NICARAGUA": {"short_label": "Nicaragua", "lat": 12.1140, "lon": -86.2362},
    "PERU": {"short_label": "Peru", "lat": -12.0464, "lon": -77.0428},
    "ROMANIA": {"short_label": "Romania", "lat": 44.4268, "lon": 26.1025},
    "RWANDA": {"short_label": "Rwanda", "lat": -1.9441, "lon": 30.0619},
    "SAUDI ARABIA": {"short_label": "Saudi Arabia", "lat": 24.7136, "lon": 46.6753},
    "SPAIN": {"short_label": "Spain", "lat": 40.4168, "lon": -3.7038},
    "TURKIYE": {"short_label": "Turkiye", "lat": 39.9334, "lon": 32.8597},
    "UNITED KINGDOM": {"short_label": "United Kingdom", "lat": 51.5074, "lon": -0.1278},
    "UZBEKISTAN": {"short_label": "Uzbekistan", "lat": 41.2995, "lon": 69.2401},
    "VENEZUELA": {"short_label": "Venezuela", "lat": 10.4806, "lon": -66.9036},
    "VIETNAM": {"short_label": "Vietnam", "lat": 21.0285, "lon": 105.8542},
}


def compact_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = " ".join(str(value).split())
    return text or None


def normalize_code(value: Any) -> str | None:
    text = compact_text(value)
    return text.upper() if text else None


def normalize_route(value: Any) -> list[str]:
    text = compact_text(value)
    if not text:
        return []
    route: list[str] = []
    for part in text.split(";"):
        code = part.strip().upper()
        if code and (not route or route[-1] != code):
            route.append(code)
    return route


def duration_hours(start: Any, end: Any) -> float | None:
    if start is None or end is None or pd.isna(start) or pd.isna(end):
        return None
    return round((end - start).total_seconds() / 3600, 2)


def sentence_total_days(days: Any, months: Any, years: Any) -> float | None:
    parts = [None if value is None or pd.isna(value) else float(value) for value in (days, months, years)]
    if all(part is None for part in parts):
        return None
    day_value, month_value, year_value = parts
    return round((day_value or 0.0) + (month_value or 0.0) * 30.4375 + (year_value or 0.0) * 365.25, 2)


def load_facility_nodes(source_path: Path) -> dict[str, dict[str, Any]]:
    df = pd.read_parquet(source_path)
    missing_columns = sorted(REQUIRED_FACILITY_COLUMNS.difference(df.columns))
    if missing_columns:
        raise ValueError(f"Facility lookup is missing required columns: {missing_columns}")

    df = df.copy()
    df["facility_code"] = df["facility_code"].map(normalize_code)
    df = df[df["facility_code"].notna()].copy()

    duplicates = sorted(df.loc[df["facility_code"].duplicated(), "facility_code"].unique())
    if duplicates:
        raise ValueError(f"Facility lookup contains duplicate codes: {duplicates}")

    records: dict[str, dict[str, Any]] = {}
    for row in df.itertuples(index=False):
        records[row.facility_code] = {
            "name": row.name,
            "short_label": row.short_label,
            "city": row.city,
            "state": row.state,
            "lat": float(row.lat),
            "lon": float(row.lon),
        }
        for optional in ("address", "coordinate_quality", "note"):
            if hasattr(row, optional):
                value = getattr(row, optional)
                if value is not None and not pd.isna(value):
                    records[row.facility_code][optional] = str(value)
    return records


def build_nodes(
    facility_codes: set[str],
    country_codes: set[str],
    facility_nodes: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    missing_facilities = sorted(code for code in facility_codes if code not in facility_nodes)
    missing_countries = sorted(code for code in country_codes if code not in COUNTRY_NODES)
    if missing_facilities or missing_countries:
        parts = []
        if missing_facilities:
            parts.append(f"Unknown facility coordinates: {missing_facilities}")
        if missing_countries:
            parts.append(f"Unknown country coordinates: {missing_countries}")
        raise ValueError(" | ".join(parts))

    nodes: list[dict[str, Any]] = []
    for code in sorted(facility_codes):
        meta = facility_nodes[code]
        nodes.append(
            {
                "id": code,
                "kind": "facility",
                "label": meta["short_label"],
                "name": meta["name"],
                "city": meta["city"],
                "state": meta["state"],
                "lat": meta["lat"],
                "lon": meta["lon"],
                "address": meta.get("address"),
                "coordinateQuality": meta.get("coordinate_quality"),
                "note": meta.get("note"),
            }
        )
    for code in sorted(country_codes):
        meta = COUNTRY_NODES[code]
        nodes.append(
            {
                "id": f"COUNTRY:{code}",
                "kind": "country",
                "label": meta["short_label"],
                "name": code.title(),
                "city": None,
                "state": None,
                "lat": meta["lat"],
                "lon": meta["lon"],
            }
        )
    return nodes


def build_payload(source_path: Path, facility_source_path: Path, years: list[int], first_facility_code: str) -> dict[str, Any]:
    facility_nodes = load_facility_nodes(facility_source_path)
    columns = [
        "stay_ID",
        "detention_facility_codes_all",
        "stay_book_in_date_time",
        "stay_book_out_date_time",
        "detention_release_reason",
        "stay_release_reason",
        "msc_charge",
        "final_charge",
        "msc_sentence_days",
        "msc_sentence_months",
        "msc_sentence_years",
        "departure_country",
        "detention_facility_code_first",
        "book_in_date_time_first",
        "book_out_date_time_first",
    ]
    df = pd.read_parquet(source_path, columns=columns)
    mask = df["stay_book_in_date_time"].dt.year.isin(years) & df["detention_facility_code_first"].fillna("").str.upper().eq(first_facility_code)
    df = df[mask].copy()
    df["route"] = df["detention_facility_codes_all"].map(normalize_route)
    df = df[df["route"].map(bool)].copy()

    year_counts = Counter(df["stay_book_in_date_time"].dt.year.astype(int))
    facility_codes: set[str] = set()
    country_codes: set[str] = set()
    stays: list[dict[str, Any]] = []

    for row in df.itertuples(index=False):
        departure_country = normalize_code(row.departure_country)
        exit_node = f"COUNTRY:{departure_country}" if departure_country else None
        path = list(row.route) + ([exit_node] if exit_node else [])
        hold_hours = duration_hours(row.book_in_date_time_first, row.book_out_date_time_first)
        start_date = row.stay_book_in_date_time.date().isoformat() if not pd.isna(row.stay_book_in_date_time) else None
        stays.append(
            {
                "id": row.stay_ID,
                "startDate": start_date,
                "path": path,
                "holdHours": hold_hours,
                "totalHours": duration_hours(row.stay_book_in_date_time, row.stay_book_out_date_time),
                "over72": bool(hold_hours is not None and hold_hours > 72),
                "finalCharge": compact_text(row.final_charge),
                "mscCharge": compact_text(row.msc_charge),
                "releaseReason": compact_text(row.detention_release_reason) or compact_text(row.stay_release_reason),
                "sentenceDays": sentence_total_days(row.msc_sentence_days, row.msc_sentence_months, row.msc_sentence_years),
            }
        )
        facility_codes.update(row.route)
        if departure_country:
            country_codes.add(departure_country)

    return {
        "metadata": {
            "years": years,
            "firstFacilityCode": first_facility_code,
            "cohortStays": len(stays),
            "minStartDate": min((stay["startDate"] for stay in stays if stay["startDate"]), default=None),
            "maxStartDate": max((stay["startDate"] for stay in stays if stay["startDate"]), default=None),
            "yearCounts": {str(year): int(year_counts[year]) for year in sorted(year_counts)},
        },
        "nodes": build_nodes(facility_codes, country_codes, facility_nodes),
        "stays": stays,
    }


def write_outputs(payload: dict[str, Any], output_json: Path, output_js: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_js.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    minimized = json.dumps(payload, separators=(",", ":"))
    with output_js.open("w", encoding="utf-8") as handle:
        handle.write(f"window.TUCHOLD_FLOW_DATA = {minimized};\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TUCHOLD movement map data for 2025-2026.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Path to the parquet file.")
    parser.add_argument("--facility-source", type=Path, default=DEFAULT_FACILITY_SOURCE, help="Path to the facility lookup parquet.")
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON, help="Where to write the JSON payload.")
    parser.add_argument("--output-js", type=Path, default=DEFAULT_OUTPUT_JS, help="Where to write the browser-ready JS payload.")
    parser.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS, help="Calendar years to include.")
    parser.add_argument("--first-facility-code", type=str, default=DEFAULT_FIRST_FACILITY_CODE, help="Only keep stays whose first facility code matches this value.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(
        source_path=args.source,
        facility_source_path=args.facility_source,
        years=sorted(set(args.years)),
        first_facility_code=args.first_facility_code.upper(),
    )
    write_outputs(payload, args.output_json, args.output_js)
    print(f"Wrote {payload['metadata']['cohortStays']} stays to {args.output_json}")
    print(f"Wrote browser payload to {args.output_js}")


if __name__ == "__main__":
    main()
