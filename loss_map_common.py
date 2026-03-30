from __future__ import annotations

import json
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


def load_positions() -> dict[str, float]:
    return json.loads(POSITIONS_FILE.read_text())


def load_blm_keys() -> list[str]:
    return list(load_positions().keys())


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
        return str(pd.Timestamp(cycle_stamp).tz_localize("UTC") if pd.Timestamp(cycle_stamp).tzinfo is None else pd.Timestamp(cycle_stamp).tz_convert("UTC"))
    if isinstance(cycle_stamp, (int, np.integer)):
        return str(pd.to_datetime(int(cycle_stamp), unit="ns", utc=True))
    return str(pd.Timestamp(cycle_stamp, tz="UTC"))


def acquisition_params(prefix: str) -> dict[str, str]:
    return {
        "losses": f"{prefix}:beamLossMeasurements_gray",
        "times": f"{prefix}:beamLossMeasurementTimes_ms",
        "channels": f"{prefix}:channelNames",
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
    if losses.shape[0] != len(channels) and losses.shape[1] == len(channels):
        losses = losses.T

    rows = [{"acquisition_prefix": prefix, "channel_name": name} for name in channels]
    return rows, times, losses


def merge_acquisitions(acquisitions: list[dict]):
    channel_rows = []
    waveform_rows = []
    common_times = None

    for acquisition in acquisitions:
        rows, times, losses = extract_waveforms(
            acquisition["prefix"],
            acquisition["losses"],
            acquisition["times"],
            acquisition["channels"],
        )
        if common_times is None:
            common_times = times
        elif len(times) != len(common_times) or not np.allclose(times, common_times):
            raise ValueError(
                f"Inconsistent time axes between BLM acquisitions, including {acquisition['prefix']}."
            )
        channel_rows.extend(rows)
        waveform_rows.append(losses)

    if common_times is None:
        raise ValueError("No BLM acquisitions loaded.")

    channel_df = pd.DataFrame(channel_rows)
    return channel_df, common_times, np.vstack(waveform_rows)


def enrich_channels(channel_df: pd.DataFrame, positions: dict[str, float]) -> pd.DataFrame:
    frame = channel_df.copy()
    frame["s_m"] = frame["channel_name"].map(positions)
    frame = frame.dropna(subset=["s_m"]).reset_index(drop=True)
    frame["s_m"] = frame["s_m"].astype(float)
    frame["is_collimator"] = frame["channel_name"].str.upper().apply(
        lambda name: any(tag in name for tag in COLLIMATOR_TAGS)
    )
    return frame.sort_values("s_m").reset_index(drop=True)


def filter_waveforms_for_known_channels(
    channel_df: pd.DataFrame, waveforms: np.ndarray, positions: dict[str, float]
):
    enriched = enrich_channels(channel_df, positions)
    row_index = {name: idx for idx, name in enumerate(channel_df["channel_name"])}
    indices = [row_index[name] for name in enriched["channel_name"]]
    return enriched, waveforms[indices, :]


def window_mask(times_ms: np.ndarray, window_ms: tuple[float, float]) -> np.ndarray:
    return (times_ms >= float(window_ms[0])) & (times_ms <= float(window_ms[1]))


def build_snapshot(
    channel_df: pd.DataFrame,
    times_ms: np.ndarray,
    waveforms: np.ndarray,
    window_ms: tuple[float, float],
    cycle_stamp,
    floor: float = PLOT_FLOOR,
) -> pd.DataFrame:
    mask = window_mask(times_ms, window_ms)
    if not np.any(mask):
        raise ValueError(f"No BLM samples found in the loss-map window {window_ms}.")

    window_times = times_ms[mask]
    window_waveforms = waveforms[:, mask]
    total_losses = np.sum(window_waveforms, axis=0)
    max_index = int(np.argmax(total_losses))
    snapshot = window_waveforms[:, max_index]
    total_loss = float(total_losses[max_index])
    norm = np.zeros_like(snapshot, dtype=float)
    if total_loss > 0.0:
        norm = snapshot / total_loss

    frame = channel_df.copy()
    frame["loss"] = np.asarray(snapshot, dtype=float)
    frame["norm_inefficiency"] = np.maximum(norm, floor)
    frame["has_signal"] = np.isfinite(snapshot) & (snapshot > 0.0)
    frame["sample_time_ms"] = float(window_times[max_index])
    frame["total_loss"] = total_loss
    frame["cycle_stamp"] = cycle_stamp
    frame["cycle_timestamp_utc"] = cycle_timestamp_utc(cycle_stamp)
    return frame


def build_repetition_dataframe(
    channel_df: pd.DataFrame,
    times_ms: np.ndarray,
    waveforms: np.ndarray,
    bct_data: dict,
    metadata: dict,
    header: dict,
) -> pd.DataFrame:
    n_channels, n_times = waveforms.shape
    repeated_meta = {f"metadata_{key}": parquet_scalar(value) for key, value in metadata.items()}
    repeated_header = {f"header_{key}": parquet_scalar(value) for key, value in header.items()}

    blm_frame = pd.DataFrame(
        {
            "record_type": "blm",
            "acquisition_prefix": np.repeat(channel_df["acquisition_prefix"].to_numpy(), n_times),
            "channel_name": np.repeat(channel_df["channel_name"].to_numpy(), n_times),
            "s_m": np.repeat(channel_df["s_m"].to_numpy(), n_times),
            "is_collimator": np.repeat(channel_df["is_collimator"].to_numpy(), n_times),
            "sample_time_ms": np.tile(times_ms, n_channels),
            "measurement_value": waveforms.reshape(-1),
            "measurement_unit": "gray",
        }
    )

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
    channel_df: pd.DataFrame,
    times_ms: np.ndarray,
    waveforms: np.ndarray,
    bct_data: dict,
    metadata: dict,
    header: dict,
) -> dict:
    return {
        "metadata": metadata,
        "header": header,
        "data": {
            "blm": {
                "acquisition_prefix": channel_df["acquisition_prefix"].tolist(),
                "channel_name": channel_df["channel_name"].tolist(),
                "s_m": channel_df["s_m"].tolist(),
                "is_collimator": channel_df["is_collimator"].tolist(),
                "sample_times_ms": np.asarray(times_ms).tolist(),
                "waveforms_gray": np.asarray(waveforms).tolist(),
            },
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

    channel_df = (
        blm[["acquisition_prefix", "channel_name", "s_m", "is_collimator"]]
        .drop_duplicates()
        .sort_values("s_m")
        .reset_index(drop=True)
    )
    times_ms = np.sort(blm["sample_time_ms"].unique().astype(float))
    pivot = (
        blm.pivot_table(
            index="channel_name",
            columns="sample_time_ms",
            values="measurement_value",
            aggfunc="first",
        )
        .reindex(channel_df["channel_name"])
        .reindex(columns=times_ms)
    )
    waveforms = pivot.to_numpy(dtype=float)
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
    return metadata, channel_df, times_ms, waveforms


def plot_snapshot(ax, snapshot: pd.DataFrame, show_labels: bool = False, floor: float = PLOT_FLOOR):
    ax.cla()
    regular = snapshot.loc[~snapshot["is_collimator"]]
    collimators = snapshot.loc[snapshot["is_collimator"]]

    if not regular.empty:
        ax.scatter(
            regular["s_m"],
            np.maximum(regular["norm_inefficiency"], floor),
            s=18,
            color="red",
        )
    if not collimators.empty:
        ax.scatter(
            collimators["s_m"],
            np.maximum(collimators["norm_inefficiency"], floor),
            s=18,
            color="black",
        )

    if show_labels:
        labelled = snapshot.loc[snapshot["has_signal"]]
        for row in labelled.itertuples(index=False):
            ax.annotate(
                row.channel_name,
                (row.s_m, max(row.norm_inefficiency, floor)),
                fontsize=6,
                rotation=45,
                alpha=0.75,
            )

    sample_time_ms = float(snapshot["sample_time_ms"].iloc[0])
    ax.set_yscale("log")
    ax.set_ylim(bottom=floor)
    ax.set_xlabel("Position along the ring [m]")
    ax.set_ylabel("Loss / total loss")
    ax.set_title(f"Loss map at {sample_time_ms:.1f} ms")
    ax.grid(True, alpha=0.3)


def plot_overlay(ax, history: list[dict], show_labels: bool = False, floor: float = PLOT_FLOOR):
    ax.cla()
    if not history:
        ax.set_xlabel("Position along the ring [m]")
        ax.set_ylabel("Loss / total loss")
        ax.set_yscale("log")
        ax.set_ylim(bottom=floor)
        ax.grid(True, alpha=0.3)
        return

    alphas = np.linspace(0.35, 1.0, num=len(history))
    sizes = np.linspace(12, 22, num=len(history))

    for alpha, size, item in zip(alphas, sizes, history):
        snapshot = item["snapshot"]
        label = item["label"]
        regular = snapshot.loc[~snapshot["is_collimator"]]
        collimators = snapshot.loc[snapshot["is_collimator"]]

        if not regular.empty:
            ax.scatter(
                regular["s_m"],
                np.maximum(regular["norm_inefficiency"], floor),
                s=size,
                color="red",
                alpha=float(alpha),
                label=f"{label} regular",
            )
        if not collimators.empty:
            ax.scatter(
                collimators["s_m"],
                np.maximum(collimators["norm_inefficiency"], floor),
                s=size,
                color="black",
                alpha=float(alpha),
                label=f"{label} collimators",
            )

    if show_labels:
        latest = history[-1]["snapshot"]
        labelled = latest.loc[latest["has_signal"]]
        for row in labelled.itertuples(index=False):
            ax.annotate(
                row.channel_name,
                (row.s_m, max(row.norm_inefficiency, floor)),
                fontsize=6,
                rotation=45,
                alpha=0.75,
            )

    ax.set_yscale("log")
    ax.set_ylim(bottom=floor)
    ax.set_xlabel("Position along the ring [m]")
    ax.set_ylabel("Loss / total loss")
    ax.set_title(f"Last {len(history)} acquisitions")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=7, loc="upper right")


def save_loss_map_pdf(
    parquet_path: Path,
    output_path: Path,
    loss_map_window_ms: tuple[float, float],
    show_labels: bool = False,
):
    metadata, channel_df, times_ms, waveforms = load_saved_repetition(parquet_path)
    cycle_stamp = metadata["cycle_stamp"]

    snapshot = build_snapshot(channel_df, times_ms, waveforms, loss_map_window_ms, cycle_stamp)

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
