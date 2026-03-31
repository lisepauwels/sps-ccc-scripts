from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PLOT_FLOOR = 1.0e-12


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect a saved loss-map parquet or JSON file.")
    parser.add_argument("path", help="Path to one saved loss-map file (.parquet or .json)")
    parser.add_argument("--plot", action="store_true", help="Also plot the integrated loss map.")
    parser.add_argument(
        "--window",
        nargs=2,
        type=float,
        metavar=("START_MS", "STOP_MS"),
        default=None,
        help="Optional cycle-time window in ms for the quick plot.",
    )
    return parser.parse_args()


def load_from_parquet(path: Path):
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Empty parquet file: {path}")

    row = frame.iloc[0].to_dict()
    metadata = {
        key.removeprefix("metadata_"): value
        for key, value in row.items()
        if key.startswith("metadata_")
    }
    header = {
        key.removeprefix("header_"): value
        for key, value in row.items()
        if key.startswith("header_")
    }
    blm = frame.loc[frame["record_type"] == "blm"].copy().reset_index(drop=True)
    bct = frame.loc[frame["record_type"] == "bct"].copy().reset_index(drop=True)
    return metadata, header, blm, bct


def load_from_json(path: Path):
    payload = json.loads(path.read_text())
    metadata = payload.get("metadata", {})
    header = payload.get("header", {})
    blm = pd.DataFrame(payload.get("data", {}).get("blm", []))
    bct_raw = payload.get("data", {}).get("bct", {})
    bct = pd.DataFrame([{"raw_bct_present": bool(bct_raw)}])
    return metadata, header, blm, bct


def load_file(path: Path):
    if path.suffix == ".parquet":
        return load_from_parquet(path)
    if path.suffix == ".json":
        return load_from_json(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def summarize(metadata, header, blm: pd.DataFrame, bct: pd.DataFrame, path: Path):
    print(f"File: {path}")
    print()
    print("Metadata")
    print("--------")
    for key in sorted(metadata):
        print(f"{key}: {metadata[key]}")

    print()
    print("Header Keys")
    print("-----------")
    for key in sorted(header):
        print(key)

    print()
    print("Data Summary")
    print("------------")
    print(f"BLM rows: {len(blm)}")
    print(f"BCT rows/info: {len(bct)}")
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


def build_lossmap(blm: pd.DataFrame, window):
    frame = blm.copy()
    if window is not None:
        frame = frame.loc[
            (frame["sample_time_ms"] >= float(window[0])) & (frame["sample_time_ms"] <= float(window[1]))
        ].copy()
    grouped = (
        frame.groupby(["channel_name", "s_m", "is_collimator"], as_index=False)["measurement_value"]
        .sum()
        .rename(columns={"measurement_value": "loss"})
        .sort_values("s_m")
        .reset_index(drop=True)
    )
    total = float(grouped["loss"].sum())
    grouped["norm_inefficiency"] = grouped["loss"] / total if total > 0.0 else 0.0
    return grouped


def plot_lossmap(lossmap: pd.DataFrame, title: str):
    regular = lossmap.loc[~lossmap["is_collimator"]]
    coll = lossmap.loc[lossmap["is_collimator"]]

    plt.figure(figsize=(12, 4.8))
    if not regular.empty:
        plt.vlines(
            regular["s_m"],
            PLOT_FLOOR,
            np.maximum(regular["norm_inefficiency"], PLOT_FLOOR),
            color="red",
        )
    if not coll.empty:
        plt.vlines(
            coll["s_m"],
            PLOT_FLOOR,
            np.maximum(coll["norm_inefficiency"], PLOT_FLOOR),
            color="black",
        )
    plt.yscale("log")
    plt.xlabel("Position along the ring [m]")
    plt.ylabel("Loss / total loss")
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def main():
    args = parse_args()
    path = Path(args.path)
    metadata, header, blm, bct = load_file(path)
    summarize(metadata, header, blm, bct, path)

    if args.plot:
        if blm.empty:
            raise SystemExit("No BLM rows available for plotting.")
        lossmap = build_lossmap(blm, args.window)
        title = f"Quick loss-map check: {path.name}"
        if args.window is not None:
            title += f" | window {args.window[0]:.0f}-{args.window[1]:.0f} ms"
        plot_lossmap(lossmap, title)


if __name__ == "__main__":
    main()
