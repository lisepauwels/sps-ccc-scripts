from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Compact inspector for one saved loss-map parquet file.")
    parser.add_argument("path", help="Path to one id*.parquet file")
    parser.add_argument("--show-header-values", action="store_true", help="Also print header values, not only header keys.")
    return parser.parse_args()


def maybe_decode_json(value):
    if not isinstance(value, str):
        return value
    value = value.strip()
    if not value:
        return value
    if value[0] not in "[{":
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def print_section(title: str):
    print()
    print(title)
    print("-" * len(title))


def main():
    args = parse_args()
    path = Path(args.path)
    frame = pd.read_parquet(path)
    if frame.empty:
        print(f"{path}: empty parquet")
        return

    first_row = frame.iloc[0].to_dict()
    metadata = {
        key.removeprefix("metadata_"): maybe_decode_json(value)
        for key, value in first_row.items()
        if key.startswith("metadata_")
    }
    header = {
        key.removeprefix("header_"): maybe_decode_json(value)
        for key, value in first_row.items()
        if key.startswith("header_")
    }

    blm = frame.loc[frame["record_type"] == "blm"].copy()
    bct = frame.loc[frame["record_type"] == "bct"].copy()

    print(f"File: {path}")
    print(f"Rows: {len(frame)}")

    print_section("Metadata")
    for key in sorted(metadata):
        print(f"{key}: {metadata[key]}")

    print_section("Header")
    if args.show_header_values:
        for key in sorted(header):
            print(f"{key}: {header[key]}")
    else:
        for key in sorted(header):
            print(key)

    print_section("Data Summary")
    print(f"BLM rows: {len(blm)}")
    print(f"BCT rows: {len(bct)}")
    if not blm.empty:
        print(f"BLM channels: {blm['channel_name'].nunique()}")
        print(f"BLM acquisitions: {blm['acquisition_prefix'].nunique()}")
        print(f"BLM sample times: {blm['sample_time_ms'].nunique()}")
        print(
            "BLM time range [ms]: "
            f"{float(blm['sample_time_ms'].min()):.3f} -> {float(blm['sample_time_ms'].max()):.3f}"
        )
        print("First 10 BLM names:")
        for name in sorted(blm["channel_name"].drop_duplicates())[:10]:
            print(f"  {name}")
    if not bct.empty:
        print(f"BCT sample times: {bct['sample_time_ms'].nunique()}")
        print(
            "BCT time range [ms]: "
            f"{float(bct['sample_time_ms'].min()):.3f} -> {float(bct['sample_time_ms'].max()):.3f}"
        )

    print_section("Columns")
    for column in frame.columns:
        print(column)


if __name__ == "__main__":
    main()
