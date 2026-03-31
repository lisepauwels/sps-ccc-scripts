from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyjapc

from bct import beam_injected
from loss_map_common import STORAGE_ROOT
from loss_map_common import acquisition_params
from loss_map_common import build_blm_frame
from loss_map_common import load_blm_sources
from loss_map_common import build_repetition_payload
from loss_map_common import build_repetition_dataframe
from loss_map_common import cycle_timestamp_utc
from loss_map_common import header_cycle_name
from loss_map_common import header_selector
from loss_map_common import load_positions
from loss_map_common import save_payload_json


THIS_TIME = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

_SPS_USER = "SPS.USER.MD2"
_ANCHOR = "SPS.BCTDC24.51454/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"

_LOSS_MAP_WINDOW_MS = (2200.0, 3200.0)
_BEAM_CHECK_BEFORE_MS = _LOSS_MAP_WINDOW_MS[1]
_SAVE_PARQUET = True
_SAVE_JSON = True


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def parse_args():
    parser = argparse.ArgumentParser(description="Save SPS loss-map BLM/BCT data into parquet files.")
    parser.add_argument("--study-name", required=True, help="Study-case directory name below ../sps-measurements/lossmaps/.")
    parser.add_argument("--repetitions", type=int, default=3, help="Number of successful repetitions to save.")
    return parser.parse_args()


def study_dir(study_name: str) -> Path:
    return STORAGE_ROOT / study_name


def repetition_path(study_name: str, repetition: int) -> Path:
    return study_dir(study_name) / f"id{repetition}.parquet"


def repetition_json_path(study_name: str, repetition: int) -> Path:
    return study_dir(study_name) / f"id{repetition}.json"


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
    blm_keys = set(positions)
    blm_sources = load_blm_sources()
    japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)

    state = {
        "repetition": 1,
        "finished": False,
        "last_accepted_cycle_stamp": None,
    }

    def skip_existing_results():
        while state["repetition"] <= args.repetitions and repetition_path(args.study_name, state["repetition"]).exists():
            state["repetition"] += 1
        if state["repetition"] > args.repetitions:
            state["finished"] = True

    def on_cycle(_param, anchor_value, anchor_header):
        if state["finished"]:
            return

        if not blm_keys:
            print_log("No BLMs found in blm_positions.json.")
            japc.stopSubscriptions()
            return

        bct_data = anchor_value
        cycle_stamp = anchor_header["cycleStamp"]
        rep = state["repetition"]
        incoming_selector = header_selector(anchor_header)

        print_log(f"Cycle {cycle_stamp}: repetition {rep}", end="")

        if incoming_selector and incoming_selector != _SPS_USER:
            print_log(f" -> SKIP. Selector {incoming_selector} does not match {_SPS_USER}.")
            return

        if state["last_accepted_cycle_stamp"] == cycle_stamp:
            print_log(" -> SKIP. Same cycleStamp as previous accepted cycle.")
            return

        if not beam_injected(bct_data, t_before_ms=_BEAM_CHECK_BEFORE_MS):
            print_log(" -> ERROR. No beam before loss-map window. Hanging on same repetition.")
            return

        acquisitions = [load_acquisition(japc, prefix) for prefix in blm_sources]
        blm_frame = build_blm_frame(acquisitions, positions)
        if blm_frame.empty:
            print_log(" -> ERROR. No SPS BLMs from blm_positions.json were found in the BA/LSS acquisitions.")
            return

        cycle_name = (
            header_cycle_name(anchor_header)
            or incoming_selector
            or ""
        )

        metadata = {
            "study_name": args.study_name,
            "selector": _SPS_USER,
            "md_user": _SPS_USER,
            "cycle_name": cycle_name,
            "repetition": rep,
            "cycle_stamp": cycle_stamp,
            "cycle_timestamp_utc": cycle_timestamp_utc(cycle_stamp),
            "bct_device": _BCT,
            "anchor_device": _ANCHOR,
            "saved_at_utc": f"{pd.Timestamp.now(tz='UTC')}".split(".")[0],
            "loss_map_window_start_ms": _LOSS_MAP_WINDOW_MS[0],
            "loss_map_window_stop_ms": _LOSS_MAP_WINDOW_MS[1],
        }

        parquet_path = repetition_path(args.study_name, rep)
        json_path = repetition_json_path(args.study_name, rep)

        if _SAVE_PARQUET:
            frame = build_repetition_dataframe(blm_frame, bct_data, metadata, anchor_header)
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(parquet_path, index=False)

        if _SAVE_JSON:
            payload = build_repetition_payload(blm_frame, bct_data, metadata, anchor_header)
            save_payload_json(json_path, payload)

        state["last_accepted_cycle_stamp"] = cycle_stamp
        saved_outputs = []
        if _SAVE_PARQUET:
            saved_outputs.append(str(parquet_path))
        if _SAVE_JSON:
            saved_outputs.append(str(json_path))
        print_log(f" -> OK. Stored {', '.join(saved_outputs)}")

        state["repetition"] += 1
        skip_existing_results()
        if state["finished"]:
            print_log("Finished requested loss-map repetitions.")
            japc.stopSubscriptions()

    skip_existing_results()
    if state["finished"]:
        print_log(f"{THIS_TIME}: All requested files already exist for study '{args.study_name}'.")
        return

    japc.subscribeParam(_ANCHOR, on_cycle, getHeader=True)
    print_log()
    print_log(f"{THIS_TIME}: Starting loss-map saver for study '{args.study_name}'")
    print_log(f"                     Repetitions: {args.repetitions}")
    print_log(f"                     Output dir: {study_dir(args.study_name)}")
    print_log(f"                     BLM keys kept from blm_positions.json: {len(blm_keys)}")
    print_log(f"                     Acquisition sources: {blm_sources}")
    print_log(f"                     Loss-map window: {_LOSS_MAP_WINDOW_MS}")
    print_log(f"                     Save parquet/json: {_SAVE_PARQUET}/{_SAVE_JSON}")
    japc.startSubscriptions()


if __name__ == "__main__":
    main()
