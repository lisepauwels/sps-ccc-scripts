from __future__ import annotations

import argparse
from collections import deque
from queue import Empty
from queue import Queue

import matplotlib.pyplot as plt
import pandas as pd
import pyjapc

from bct import beam_injected
from loss_map_common import acquisition_params
from loss_map_common import build_blm_frame
from loss_map_common import build_snapshot
from loss_map_common import header_selector
from loss_map_common import load_blm_sources
from loss_map_common import load_positions
from loss_map_common import plot_total_losses
from loss_map_common import plot_snapshot
from loss_map_common import total_losses_trace


THIS_TIME = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

_SPS_USER = "SPS.USER.MD2"
_ANCHOR = "SPS.BCTDC24.51454/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"

_LOSS_MAP_WINDOW_MS = (1015.0, 3200.0)
_SHOW_BLM_LABELS = False  # Set to True only if you explicitly want labels on the plot.


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def parse_args():
    return argparse.ArgumentParser(description="Live SPS loss-map plotter.").parse_args()


def load_acquisition(japc, prefix: str):
    params = acquisition_params(prefix)
    return {
        "prefix": prefix,
        "losses": japc.getParam(params["losses"]),
        "times": japc.getParam(params["times"]),
        "channels": japc.getParam(params["channels"]),
    }


def main():
    parse_args()
    positions = load_positions()
    blm_keys = set(positions)
    blm_sources = load_blm_sources()
    japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
    map_history = deque(maxlen=3)
    updates = Queue(maxsize=1)
    last_accepted_cycle_stamp = {"value": None}

    plt.ion()
    figures_axes = [plt.subplots(figsize=(12, 4.8), num=f"Loss Map {idx}") for idx in range(1, 4)]
    trace_figure, trace_axis = plt.subplots(figsize=(12, 4.8), num="Latest Total Losses")

    def on_cycle(_param, anchor_value, anchor_header):
        if not blm_keys:
            print_log("No BLMs found in blm_positions.json.")
            japc.stopSubscriptions()
            return

        bct_data = anchor_value
        cycle_stamp = anchor_header["cycleStamp"]
        incoming_selector = header_selector(anchor_header)
        if incoming_selector and incoming_selector != _SPS_USER:
            print_log(f"Cycle {cycle_stamp}: skipping selector {incoming_selector}.")
            return
        if last_accepted_cycle_stamp["value"] == cycle_stamp:
            print_log(f"Cycle {cycle_stamp}: skipping duplicate cycleStamp.")
            return
        if not beam_injected(bct_data, t_before_ms=_LOSS_MAP_WINDOW_MS[1]):
            print_log(f"Cycle {cycle_stamp}: no beam. Waiting for a valid cycle.")
            return

        acquisitions = [load_acquisition(japc, prefix) for prefix in blm_sources]
        blm_frame = build_blm_frame(acquisitions, positions)
        if blm_frame.empty:
            print_log(f"Cycle {cycle_stamp}: no SPS BLMs matched blm_positions.json.")
            return

        trace = total_losses_trace(blm_frame, _LOSS_MAP_WINDOW_MS)
        snapshot = build_snapshot(blm_frame, _LOSS_MAP_WINDOW_MS, cycle_stamp)
        payload = {
            "cycle_stamp": cycle_stamp,
            "trace": trace,
            "snapshot": snapshot,
        }
        last_accepted_cycle_stamp["value"] = cycle_stamp
        while not updates.empty():
            try:
                updates.get_nowait()
            except Empty:
                break
        updates.put_nowait(payload)

    japc.subscribeParam(_ANCHOR, on_cycle, getHeader=True)
    print_log()
    print_log(f"{THIS_TIME}: Starting live loss-map plotter")
    print_log(f"                     BLM keys kept from blm_positions.json: {len(blm_keys)}")
    print_log(f"                     Acquisition sources: {blm_sources}")
    print_log(f"                     Loss-map window: {_LOSS_MAP_WINDOW_MS}")
    japc.startSubscriptions()

    try:
        while True:
            latest = None
            try:
                while True:
                    latest = updates.get_nowait()
            except Empty:
                pass

            if latest is not None:
                map_history.append(
                    {
                        "label": f"cycle {latest['cycle_stamp']}",
                        "snapshot": latest["snapshot"],
                    }
                )

                plot_total_losses(
                    trace_axis,
                    [{"label": f"cycle {latest['cycle_stamp']}", "trace": latest["trace"]}],
                )
                trace_figure.tight_layout()
                trace_figure.canvas.draw_idle()

                ordered_history = list(map_history)[::-1]
                for idx, (figure, axis) in enumerate(figures_axes):
                    axis.cla()
                    if idx < len(ordered_history):
                        item = ordered_history[idx]
                        plot_snapshot(axis, item["snapshot"], show_labels=_SHOW_BLM_LABELS)
                        axis.set_title(f"Loss map {idx + 1} | {item['label']}")
                    else:
                        axis.set_xlabel("Position along the ring [m]")
                        axis.set_ylabel("Loss / total loss")
                        axis.set_yscale("log")
                        axis.grid(True, alpha=0.3)
                    figure.tight_layout()
                    figure.canvas.draw_idle()

            plt.pause(0.1)
    finally:
        japc.stopSubscriptions()


if __name__ == "__main__":
    main()
