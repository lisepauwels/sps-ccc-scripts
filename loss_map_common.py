from __future__ import annotations

import json
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bct import time_ms as bct_time_ms
from bct import total_intensity
from json_utils import json_default


POSITIONS_FILE = Path("blm_positions.json")
STORAGE_ROOT = Path("../sps-measurements/lossmaps")
COLLIMATOR_TAGS = ("TCSM", "TIDP")
PLOT_FLOOR = 1.0e-12
BLM_ACQUISITION_SOURCES = [f"BLRSPS_BA{i}/ExpertAcquisition" for i in range(1, 7)] + [
    f"BLRSPS_LSS{i}/ExpertAcquisition" for i in (1, 2, 4, 5, 6)
]


def load_positions() -> dict[str, float]:
    return json.loads(POSITIONS_FILE.read_text())


def load_blm_keys() -> set[str]:
    return set(load_positions().keys())


def load_blm_sources() -> list[str]:
    return list(BLM_ACQUISITION_SOURCES)


def to_array(raw):
    if isinstance(raw, dict) and "value" in raw:
        raw = raw["value"]
    return np.asarray(raw)


def parquet_scalar(value):
    if isinstance(value, (np.generic,)):
        return value.item()
    if isinstance(value, (dict, list, tuple, np.ndarray)):
        return json.dumps(value, default=json_default)
    return value


def cycle_timestamp_utc(cycle_stamp) -> str:
    if isinstance(cycle_stamp, str):
        ts = pd.Timestamp(cycle_stamp)
        return str(ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC"))
    if isinstance(cycle_stamp, (int, np.integer)):
        return str(pd.to_datetime(int(cycle_stamp), unit="ns", utc=True))
    ts = pd.Timestamp(cycle_stamp)
    return str(ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC"))


def header_selector(header: dict) -> str:
    for key in ("selector", "japc_selector", "user"):
        value = header.get(key)
        if value:
            return str(value)
    return ""


def header_cycle_name(header: dict) -> str:
    for key in ("cycleName", "cycle_name", "cycle"):
        value = header.get(key)
        if value:
            return str(value)
    return ""


def acquisition_params(prefix: str) -> dict[str, str]:
    return {
        "losses": f"{prefix}#beamLossMeasurements_gray",
        "times": f"{prefix}#beamLossMeasurementTimes_ms",
        "channels": f"{prefix}#channelNames",
    }


def extract_waveforms(prefix: str, losses_raw, times_raw, channels_raw):
    losses = to_array(losses_raw)
    times = to_array(times_raw).astype(float)
    channels = channels_raw
    if isinstance(channels, dict) and "value" in channels:
        channels = channels["value"]
    channels = [str(name) for name in channels]

    if losses.ndim == 1:
        losses = losses[np.newaxis, :]
    if losses.ndim != 2:
        raise ValueError(f"Unexpected BLM loss array rank for {prefix}: {losses.shape}")
    if losses.shape[0] == len(times) and losses.shape[1] == len(channels):
        losses = losses.T
    elif losses.shape[0] == len(channels):
        pass
    elif losses.shape[1] == len(channels):
        losses = losses.T

    if losses.shape[0] != len(channels):
        n_channels = min(losses.shape[0], len(channels))
        warnings.warn(
            f"{prefix}: channel count mismatch between channelNames ({len(channels)}) "
            f"and beam losses ({losses.shape[0]}). Trimming to {n_channels}.",
            stacklevel=2,
        )
        channels = channels[:n_channels]
        losses = losses[:n_channels, :]

    if losses.shape[1] != len(times):
        n_times = min(losses.shape[1], len(times))
        warnings.warn(
            f"{prefix}: timestamp count mismatch between beamLossMeasurementTimes_ms ({len(times)}) "
            f"and beam losses ({losses.shape[1]}). Trimming to {n_times}.",
            stacklevel=2,
        )
        times = times[:n_times]
        losses = losses[:, :n_times]

    rows = [{"acquisition_prefix": prefix, "channel_name": name} for name in channels]
    return rows, times, losses


def enrich_channels(channel_df: pd.DataFrame, positions: dict[str, float]) -> pd.DataFrame:
    frame = channel_df.copy()
    frame["s_m"] = frame["channel_name"].map(positions)
    frame = frame.dropna(subset=["s_m"]).reset_index(drop=True)
    frame["s_m"] = frame["s_m"].astype(float)
    frame["is_collimator"] = frame["channel_name"].str.upper().apply(
        lambda name: any(tag in name for tag in COLLIMATOR_TAGS)
    )
    return frame.sort_values(["s_m", "channel_name", "acquisition_prefix"]).reset_index(drop=True)


def build_blm_frame(acquisitions: list[dict], positions: dict[str, float]) -> pd.DataFrame:
    frames = []
    for acquisition in acquisitions:
        rows, times, losses = extract_waveforms(
            acquisition["prefix"],
            acquisition["losses"],
            acquisition["times"],
            acquisition["channels"],
        )
        channel_df_all = pd.DataFrame(rows)
        channel_df = enrich_channels(channel_df_all, positions)
        if channel_df.empty:
            continue

        row_index = {row["channel_name"]: idx for idx, row in enumerate(rows)}
        indices = [row_index[name] for name in channel_df["channel_name"]]
        filtered_losses = np.asarray(losses[indices, :], dtype=float)
        n_channels, n_times = filtered_losses.shape
        if n_times != len(times):
            raise ValueError(
                f"Unexpected filtered waveform shape for {acquisition['prefix']}: "
                f"{filtered_losses.shape} with {len(times)} timestamps."
            )

        frames.append(
            pd.DataFrame(
                {
                    "record_type": "blm",
                    "acquisition_prefix": np.repeat(channel_df["acquisition_prefix"].to_numpy(), n_times),
                    "channel_name": np.repeat(channel_df["channel_name"].to_numpy(), n_times),
                    "s_m": np.repeat(channel_df["s_m"].to_numpy(), n_times),
                    "is_collimator": np.repeat(channel_df["is_collimator"].to_numpy(), n_times),
                    "sample_time_ms": np.tile(times, n_channels),
                    "measurement_value": filtered_losses.reshape(-1),
                    "measurement_unit": "gray",
                }
            )
        )

    if not frames:
        return pd.DataFrame(
            columns=[
                "record_type",
                "acquisition_prefix",
                "channel_name",
                "s_m",
                "is_collimator",
                "sample_time_ms",
                "measurement_value",
                "measurement_unit",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def window_mask(times_ms: np.ndarray, window_ms: tuple[float, float] | None) -> np.ndarray:
    if window_ms is None:
        return np.ones_like(times_ms, dtype=bool)
    return (times_ms >= float(window_ms[0])) & (times_ms <= float(window_ms[1]))


def build_snapshot(
    blm_frame: pd.DataFrame,
    window_ms: tuple[float, float] | None,
    cycle_stamp,
    floor: float = PLOT_FLOOR,
) -> pd.DataFrame:
    windowed = blm_frame.copy()
    if window_ms is not None:
        windowed = windowed.loc[window_mask(windowed["sample_time_ms"].to_numpy(dtype=float), window_ms)].copy()
    if windowed.empty:
        raise ValueError(f"No BLM samples found in the requested loss-map window {window_ms}.")

    grouped = (
        windowed.groupby(["acquisition_prefix", "channel_name", "s_m", "is_collimator"], as_index=False)[
            "measurement_value"
        ]
        .sum()
        .rename(columns={"measurement_value": "loss"})
        .sort_values("s_m")
        .reset_index(drop=True)
    )
    total_loss = float(grouped["loss"].sum())
    norm = np.zeros(len(grouped), dtype=float)
    if total_loss > 0.0:
        norm = grouped["loss"].to_numpy(dtype=float) / total_loss

    frame = grouped.copy()
    frame["norm_inefficiency"] = np.maximum(norm, floor)
    frame["has_signal"] = np.isfinite(frame["loss"]) & (frame["loss"] > 0.0)
    frame["sample_time_ms"] = np.nan
    frame["total_loss"] = total_loss
    frame["cycle_stamp"] = cycle_stamp
    frame["cycle_timestamp_utc"] = cycle_timestamp_utc(cycle_stamp)
    return frame


def total_losses_trace(blm_frame: pd.DataFrame, window_ms: tuple[float, float] | None = None) -> pd.DataFrame:
    frame = blm_frame.copy()
    if window_ms is not None:
        frame = frame.loc[window_mask(frame["sample_time_ms"].to_numpy(dtype=float), window_ms)].copy()
    if frame.empty:
        return pd.DataFrame({"sample_time_ms": [], "total_loss": []})
    return (
        frame.groupby("sample_time_ms", as_index=False)["measurement_value"]
        .sum()
        .rename(columns={"measurement_value": "total_loss"})
        .sort_values("sample_time_ms")
        .reset_index(drop=True)
    )


def build_repetition_dataframe(
    blm_frame: pd.DataFrame,
    bct_data: dict,
    metadata: dict,
    header: dict,
) -> pd.DataFrame:
    repeated_meta = {f"metadata_{key}": parquet_scalar(value) for key, value in metadata.items()}
    repeated_header = {f"header_{key}": parquet_scalar(value) for key, value in header.items()}

    bct_frame = pd.DataFrame(
        {
            "record_type": "bct",
            "acquisition_prefix": metadata["bct_device"],
            "channel_name": metadata["bct_device"],
            "s_m": np.nan,
            "is_collimator": False,
            "sample_time_ms": bct_time_ms(bct_data),
            "measurement_value": total_intensity(bct_data),
            "measurement_unit": "total_intensity",
        }
    )

    frame = pd.concat([blm_frame, bct_frame], ignore_index=True, sort=False)
    for key, value in repeated_meta.items():
        frame[key] = value
    for key, value in repeated_header.items():
        frame[key] = value
    return frame


def build_repetition_payload(
    blm_frame: pd.DataFrame,
    bct_data: dict,
    metadata: dict,
    header: dict,
) -> dict:
    return {
        "metadata": metadata,
        "header": header,
        "data": {
            "blm": blm_frame.to_dict(orient="records"),
            "bct": bct_data,
        },
    }


def save_payload_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fp:
        json.dump(payload, fp, indent=2, default=json_default)


def load_saved_repetition(path: Path):
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Empty parquet file: {path}")

    blm = frame.loc[frame["record_type"] == "blm"].copy()
    if blm.empty:
        raise ValueError(f"No BLM data stored in {path}")

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
    metadata["header"] = header
    return metadata, blm.reset_index(drop=True)


def plot_snapshot(ax, snapshot: pd.DataFrame, show_labels: bool = False, floor: float = PLOT_FLOOR):
    ax.cla()
    regular = snapshot.loc[~snapshot["is_collimator"]]
    collimators = snapshot.loc[snapshot["is_collimator"]]

    if not regular.empty:
        ax.vlines(
            regular["s_m"],
            floor,
            np.maximum(regular["norm_inefficiency"], floor),
            color="red",
            linewidth=1.4,
        )
    if not collimators.empty:
        ax.vlines(
            collimators["s_m"],
            floor,
            np.maximum(collimators["norm_inefficiency"], floor),
            color="black",
            linewidth=1.8,
        )

    if show_labels:
        labelled = snapshot.loc[snapshot["has_signal"]]
        for row in labelled.itertuples(index=False):
            ax.annotate(
                row.channel_name,
                (row.s_m, max(float(row.norm_inefficiency), floor)),
                fontsize=6,
                rotation=45,
                alpha=0.75,
            )

    ax.set_yscale("log")
    ax.set_ylim(bottom=floor)
    ax.set_xlabel("Position along the ring [m]")
    ax.set_ylabel("Loss / total loss")
    ax.set_title("Integrated loss map in selected cycle window")
    ax.grid(True, alpha=0.3)


def plot_total_losses(ax, history: list[dict], floor: float = PLOT_FLOOR):
    ax.cla()
    if not history:
        ax.set_xlabel("Cycle time [ms]")
        ax.set_ylabel("Total loss [gray]")
        ax.set_yscale("log")
        ax.set_ylim(bottom=floor)
        ax.grid(True, alpha=0.3)
        return

    alphas = np.linspace(0.35, 1.0, num=len(history))
    widths = np.linspace(1.0, 2.0, num=len(history))
    for alpha, width, item in zip(alphas, widths, history):
        trace = item["trace"]
        ax.plot(
            trace["sample_time_ms"],
            np.maximum(trace["total_loss"], floor),
            alpha=float(alpha),
            linewidth=float(width),
            label=item["label"],
            color="tab:red",
        )

    ax.set_xlabel("Cycle time [ms]")
    ax.set_ylabel("Total loss [gray]")
    ax.set_yscale("log")
    ax.set_ylim(bottom=floor)
    ax.set_title(f"Last {len(history)} acquisitions: total losses")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=7, loc="upper right")


def save_loss_map_pdf(
    parquet_path: Path,
    output_path: Path,
    loss_map_window_ms: tuple[float, float] | None,
    show_labels: bool = False,
):
    metadata, blm_frame = load_saved_repetition(parquet_path)
    cycle_stamp = metadata["cycle_stamp"]

    snapshot = build_snapshot(blm_frame, loss_map_window_ms, cycle_stamp)

    figure, axis = plt.subplots(1, 1, figsize=(12, 4.8))
    plot_snapshot(axis, snapshot, show_labels=show_labels)
    figure.suptitle(
        f"{metadata['study_name']} | rep {int(metadata['repetition'])} | {metadata['cycle_timestamp_utc']}",
        fontsize=11,
    )
    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=300)
    plt.close(figure)
