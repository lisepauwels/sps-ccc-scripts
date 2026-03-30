from __future__ import annotations

import argparse
from collections import deque

import matplotlib.pyplot as plt
import pandas as pd
import pyjapc

from bct import beam_injected
from loss_map_common import acquisition_params
from loss_map_common import build_snapshot
from loss_map_common import filter_waveforms_for_known_channels
from loss_map_common import load_blm_keys
from loss_map_common import load_positions
from loss_map_common import merge_acquisitions
from loss_map_common import plot_overlay


THIS_TIME = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

_SPS_USER = "SPS.USER.MD2"
_ANCHOR = "SPS.BCTDC24.51454/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"

_LOSS_MAP_WINDOW_MS = (2200.0, 3200.0)
_SHOW_BLM_LABELS = True  # Set to False if the annotations are too heavy.


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
    args = parse_args()
    positions = load_positions()
    blm_keys = load_blm_keys()
    japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
    history = deque(maxlen=3)

    plt.ion()
    figure, axis = plt.subplots(figsize=(12, 5))

    def on_cycle(_param, _anchor_value, anchor_header):
        if not blm_keys:
            print_log("No BLMs found in blm_positions.json.")
            japc.stopSubscriptions()
            return

        bct_data = japc.getParam(_BCT)
        cycle_stamp = anchor_header["cycleStamp"]
        if not beam_injected(bct_data, t_before_ms=_LOSS_MAP_WINDOW_MS[1]):
            print_log(f"Cycle {cycle_stamp}: no beam. Waiting for a valid cycle.")
            return

        acquisitions = [load_acquisition(japc, prefix) for prefix in blm_keys]
        channel_df, times_ms, waveforms = merge_acquisitions(acquisitions)
        channel_df, waveforms = filter_waveforms_for_known_channels(channel_df, waveforms, positions)

        snapshot = build_snapshot(channel_df, times_ms, waveforms, _LOSS_MAP_WINDOW_MS, cycle_stamp)
        history.append(
            {
                "label": f"acq {len(history) + 1}: {snapshot['sample_time_ms'].iloc[0]:.0f} ms",
                "snapshot": snapshot,
            }
        )

        plot_overlay(axis, list(history), show_labels=_SHOW_BLM_LABELS)
        figure.tight_layout()
        figure.canvas.draw()
        figure.canvas.flush_events()
        print_log(f"Cycle {cycle_stamp}: updated live loss-map overlay.")

    japc.subscribeParam(_ANCHOR, on_cycle, getHeader=True)
    print_log()
    print_log(f"{THIS_TIME}: Starting live loss-map plotter")
    print_log(f"                     BLM keys from blm_positions.json: {len(blm_keys)}")
    print_log(f"                     Loss-map window: {_LOSS_MAP_WINDOW_MS}")
    japc.startSubscriptions()


if __name__ == "__main__":
    main()
