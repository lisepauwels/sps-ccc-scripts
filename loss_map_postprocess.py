from __future__ import annotations

import argparse

from loss_map_common import STORAGE_ROOT
from loss_map_common import save_loss_map_pdf


_LOSS_MAP_WINDOW_MS = (2200.0, 3200.0)
_SHOW_BLM_LABELS = True  # Set to False if you want to suppress BLM names.


def parse_args():
    parser = argparse.ArgumentParser(description="Generate PDF loss maps from saved repetition parquet files.")
    parser.add_argument("--study-name", required=True, help="Study-case directory name below ../sps-measurements/lossmaps/.")
    return parser.parse_args()


def main():
    args = parse_args()
    study_dir = STORAGE_ROOT / args.study_name
    parquet_files = sorted(study_dir.glob("id*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No parquet files found in {study_dir}")

    for parquet_path in parquet_files:
        output_path = parquet_path.with_name(f"{parquet_path.stem}_lossmap.pdf")
        save_loss_map_pdf(
            parquet_path,
            output_path,
            loss_map_window_ms=_LOSS_MAP_WINDOW_MS,
            show_labels=_SHOW_BLM_LABELS,
        )
        print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
