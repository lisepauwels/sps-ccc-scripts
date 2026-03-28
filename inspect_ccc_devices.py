from __future__ import annotations

import json
from pathlib import Path

import pyjapc

from json_utils import json_default


# Fill this list on SWAN or in the CCC with the candidate devices you want to inspect.
CANDIDATE_DEVICES = [
    "SPS.BQ.CONT/ContinuousAcquisition",
    "SPS.BCTDC24.51454/Acquisition",
    "SpsLowLevelRF/DpOverPOffset",
    "SA.RevFreq-ACQ/Acquisition",
    # Add more candidates here, for example:
    # "some/other/BBQAcquisition",
    # "some/tune/reference/device",
]

SPS_USER = "SPS.USER.MD2"
OUTPUT_DIR = Path("inspection")


def summarize(value, depth=0):
    if depth > 2:
        return type(value).__name__
    if isinstance(value, dict):
        return {key: summarize(val, depth + 1) for key, val in list(value.items())[:20]}
    if isinstance(value, (list, tuple)):
        return {
            "type": type(value).__name__,
            "len": len(value),
            "first_item": summarize(value[0], depth + 1) if value else None,
        }
    shape = getattr(value, "shape", None)
    if shape is not None:
        return {"type": type(value).__name__, "shape": tuple(shape)}
    return {"type": type(value).__name__, "repr": repr(value)[:200]}


def main():
    japc = pyjapc.PyJapc(SPS_USER, incaAcceleratorName=None)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for device in CANDIDATE_DEVICES:
        safe_name = device.replace("/", "__")
        try:
            raw = japc.getParam(device)
            payload = {
                "device": device,
                "summary": summarize(raw),
                "raw": raw,
            }
            with (OUTPUT_DIR / f"{safe_name}.json").open("w") as fp:
                json.dump(payload, fp, indent=2, default=json_default)
            print(f"OK  {device}")
        except Exception as exc:
            payload = {
                "device": device,
                "error": repr(exc),
            }
            with (OUTPUT_DIR / f"{safe_name}.json").open("w") as fp:
                json.dump(payload, fp, indent=2, default=json_default)
            print(f"ERR {device}: {exc}")


if __name__ == "__main__":
    main()
