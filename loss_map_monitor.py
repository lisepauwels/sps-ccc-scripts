from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyjapc

from json_utils import json_default


studyname = "SPS live loss-map monitor"
this_time = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]


# ============================
# ========= SETTINGS  ========
# ============================

_SPS_USER = "SPS.USER.MD2"
_POSITIONS_FILE = Path("blm_positions.json")

# Fill these with the BLR acquisition prefixes available on site.
# Example format from SWAN:
#   BLRSPS_<pos>:ExpertAcquisition
_BLM_ACQUISITIONS = [
    # "BLRSPS_01:ExpertAcquisition",
    # "BLRSPS_02:ExpertAcquisition",
]

_ANCHOR = "SPS.BCTDC24.51454/Acquisition"
_SAVE_RESULTS = True
_RESULTS_DIR = Path("results/loss_maps")

# Time window inside the cycle used for saving/plotting the sweep.
_WINDOW_START_MS = 300
_WINDOW_STOP_MS = 3000
_PLOT_FLOOR = 1.0e-6


# ============================
# ========= HELPERS ==========
# ============================

positions = json.loads(_POSITIONS_FILE.read_text())
japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
plt.ion()
figure, axis = plt.subplots(figsize=(12, 4))


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def acquisition_params(prefix):
    return {
        "losses": f"{prefix}:beamLossMeasurements_gray",
        "times": f"{prefix}:beamLossMeasurementTimes_ms",
        "channels": f"{prefix}:channelNames",
    }


def load_acquisition(prefix):
    params = acquisition_params(prefix)
    return {
        "prefix": prefix,
        "losses": japc.getParam(params["losses"]),
        "times": japc.getParam(params["times"]),
        "channels": japc.getParam(params["channels"]),
    }


def to_array(raw):
    if isinstance(raw, dict) and "value" in raw:
        raw = raw["value"]
    return np.asarray(raw)


def extract_waveforms(acquisition):
    losses = to_array(acquisition["losses"])
    times = to_array(acquisition["times"]).astype(float)
    channels = acquisition["channels"]
    if isinstance(channels, dict) and "value" in channels:
        channels = channels["value"]
    channels = [str(name) for name in channels]

    if losses.ndim == 1:
        losses = losses[np.newaxis, :]
    if losses.shape[0] != len(channels) and losses.shape[1] == len(channels):
        losses = losses.T

    return channels, times, losses


def merge_acquisitions(acquisitions):
    channel_names = []
    waveform_rows = []
    common_times = None

    for acquisition in acquisitions:
        channels, times, losses = extract_waveforms(acquisition)
        if common_times is None:
            common_times = times
        else:
            if len(times) != len(common_times) or not np.allclose(times, common_times):
                raise ValueError(f"Inconsistent time axes between BLM acquisitions, including {acquisition['prefix']}.")
        channel_names.extend(channels)
        waveform_rows.append(losses)

    if common_times is None:
        raise ValueError("No BLM acquisitions loaded.")

    return channel_names, common_times, np.vstack(waveform_rows)


def select_time_window(times_ms, waveforms):
    mask = (times_ms >= _WINDOW_START_MS) & (times_ms <= _WINDOW_STOP_MS)
    return times_ms[mask], waveforms[:, mask]


def max_loss_snapshot(channel_names, times_ms, waveforms):
    total_losses = np.sum(waveforms, axis=0)
    max_index = int(np.argmax(total_losses))
    snapshot = waveforms[:, max_index]
    snapshot_time = float(times_ms[max_index])

    points = []
    for channel_name, loss_value in zip(channel_names, snapshot):
        if channel_name not in positions:
            continue
        has_signal = np.isfinite(loss_value) and float(loss_value) > 0.0
        points.append(
            {
                "channel": channel_name,
                "s": float(positions[channel_name]),
                "loss": float(loss_value) if np.isfinite(loss_value) else 0.0,
                "has_signal": bool(has_signal),
            }
        )

    points.sort(key=lambda item: item["s"])
    return snapshot_time, points, total_losses


def save_snapshot(cycle_stamp, channel_names, times_ms, waveforms, snapshot_time_ms, points):
    if not _SAVE_RESULTS:
        return

    payload = {
        "studyname": studyname,
        "selector": _SPS_USER,
        "window_start_ms": _WINDOW_START_MS,
        "window_stop_ms": _WINDOW_STOP_MS,
        "cycle_stamp": cycle_stamp,
        "saved_at_utc": f"{pd.Timestamp.now(tz='UTC')}".split(".")[0],
        "channel_names": channel_names,
        "times_ms": times_ms,
        "waveforms_gray": waveforms,
        "snapshot_time_ms": snapshot_time_ms,
        "snapshot_points": points,
    }

    path = _RESULTS_DIR / f"{cycle_stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fp:
        json.dump(payload, fp, indent=2, default=json_default)
    print_log(f"Stored {path}")


def plot_snapshot(snapshot_time_ms, points):
    axis.cla()
    if points:
        active_points = [point for point in points if point["has_signal"]]
        inactive_points = [point for point in points if not point["has_signal"]]

        if active_points:
            s_active = [point["s"] for point in active_points]
            losses_active = [max(point["loss"], _PLOT_FLOOR) for point in active_points]
            axis.scatter(s_active, losses_active, s=14, color="black")

        if inactive_points:
            s_inactive = [point["s"] for point in inactive_points]
            losses_inactive = [_PLOT_FLOOR for _ in inactive_points]
            axis.scatter(s_inactive, losses_inactive, s=18, color="blue")

    axis.set_yscale("log")
    axis.set_ylim(bottom=_PLOT_FLOOR)
    axis.set_xlabel("S position [m]")
    axis.set_ylabel("BLM loss [gray]")
    axis.set_title(f"Loss map at {snapshot_time_ms:.1f} ms")
    axis.grid(True, alpha=0.3)
    figure.tight_layout()
    figure.canvas.draw()
    figure.canvas.flush_events()


def on_cycle(_param, anchor_value, anchor_header):
    if not _BLM_ACQUISITIONS:
        print_log("No BLM acquisitions configured. Fill _BLM_ACQUISITIONS first.")
        japc.stopSubscriptions()
        return

    acquisitions = [load_acquisition(prefix) for prefix in _BLM_ACQUISITIONS]
    channel_names, times_ms, waveforms = merge_acquisitions(acquisitions)
    times_ms, waveforms = select_time_window(times_ms, waveforms)
    if times_ms.size == 0:
        print_log(f"Cycle {anchor_header['cycleStamp']}: no BLM samples in requested time window.")
        return

    snapshot_time_ms, points, total_losses = max_loss_snapshot(channel_names, times_ms, waveforms)
    save_snapshot(anchor_header["cycleStamp"], channel_names, times_ms, waveforms, snapshot_time_ms, points)
    plot_snapshot(snapshot_time_ms, points)
    print_log(
        f"Cycle {anchor_header['cycleStamp']}: "
        f"snapshot at {snapshot_time_ms:.1f} ms, "
        f"max total loss {float(np.max(total_losses)):.3e}"
    )


japc.subscribeParam(_ANCHOR, on_cycle, getHeader=True)

print_log()
print_log(f"{this_time}: Starting loss-map monitor")
print_log(f"                     Time window: {_WINDOW_START_MS} ms to {_WINDOW_STOP_MS} ms")
print_log(f"                     Acquisitions: {_BLM_ACQUISITIONS}")
japc.startSubscriptions()
