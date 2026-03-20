from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]

    start_col = next(
        (
            column
            for column in ["started_at", "start_time", "start_time_and_date"]
            if column in frame.columns
        ),
        None,
    )
    end_col = next(
        (
            column
            for column in ["ended_at", "end_time", "end_time_and_date"]
            if column in frame.columns
        ),
        None,
    )

    frame["started_at"] = pd.to_datetime(frame[start_col], errors="coerce") if start_col else pd.NaT
    frame["ended_at"] = pd.to_datetime(frame[end_col], errors="coerce") if end_col else pd.NaT
    frame["duration_min"] = ((frame["ended_at"] - frame["started_at"]).dt.total_seconds() / 60.0).fillna(0)
    frame["duration_min"] = frame["duration_min"].clip(lower=0)
    frame["weekday_num"] = frame["started_at"].dt.weekday.fillna(-1).astype(int)
    frame["start_hour"] = frame["started_at"].dt.hour.fillna(-1).astype(int)

    start_station_col = next(
        (
            column
            for column in ["start_station_name", "from_station_name"]
            if column in frame.columns
        ),
        None,
    )
    end_station_col = next(
        (
            column
            for column in ["end_station_name", "to_station_name"]
            if column in frame.columns
        ),
        None,
    )
    frame["pair"] = (
        frame[start_station_col].astype(str).fillna("Unknown") + " -> " + frame[end_station_col].astype(str).fillna("Unknown")
        if start_station_col and end_station_col
        else "Unknown -> Unknown"
    )
    return frame



def make_cases(input_csv: Path, output_dir: Path) -> tuple[Path, Path]:
    df = pd.read_csv(input_csv)
    frame = normalize_columns(df)

    heavy_pool = frame[
        (frame["weekday_num"].between(0, 4))
        & ((frame["start_hour"].between(7, 9)) | (frame["start_hour"].between(16, 18)))
        & (frame["duration_min"].between(8, 35))
    ].copy()
    if heavy_pool.empty:
        heavy_pool = frame[frame["duration_min"].between(8, 35)].copy()
    best_pair = heavy_pool["pair"].value_counts().idxmax()
    heavy_case = heavy_pool[heavy_pool["pair"] == best_pair].head(40)
    if len(heavy_case) < 20:
        heavy_case = heavy_pool.head(40)

    light_pool = frame[
        (frame["weekday_num"].between(5, 6))
        & (frame["start_hour"].between(11, 15))
        & (frame["duration_min"].between(1, 10))
    ].copy()
    if light_pool.empty:
        light_pool = frame[frame["duration_min"].between(1, 10)].copy()
    light_case = light_pool.sort_values("duration_min").head(6)

    output_dir.mkdir(parents=True, exist_ok=True)
    heavy_path = output_dir / "case_membership_wins.csv"
    light_path = output_dir / "case_pay_per_use_wins.csv"

    original_heavy = df.loc[heavy_case.index]
    original_light = df.loc[light_case.index]
    original_heavy.to_csv(heavy_path, index=False)
    original_light.to_csv(light_path, index=False)
    return heavy_path, light_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create two Bay Wheels acceptance-case CSVs.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("./prepared_cases"))
    args = parser.parse_args()
    heavy, light = make_cases(args.input_csv, args.output_dir)
    print(f"Created: {heavy}")
    print(f"Created: {light}")
