from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyjapc

import bbq
from bct import INJECTION_TIME_MS, beam_injected
from json_utils import json_default


studydesc = "dp/p scan with full BBQ and BCT saving"
shortname = "DPP_BBQ_SCAN"
repetitions = 3
this_time = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]


# ============================
# ========= SETTINGS  ========
# ============================

_SPS_USER = "SPS.USER.MD2"
_CYCLE_NAME = "MD_26_L4800_Q20_North_Extraction_2026_V1"

_BBQ = "SPS.BQ.CONT/ContinuousAcquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"
_DPP = "SpsLowLevelRF/DpOverPOffset"
_RADIAL = "SpsLowLevelRF/RadialSteering"
_REVFREQ = "SA.RevFreq-ACQ/Acquisition"

# Optional tune-reference devices to save the starting machine tune settings.
# Leave these as None until you confirm the correct CCC device names.
_QH_REF = None
_QV_REF = None

_CYCLE_OFFSET_MS = 1015
_START_DPP_MS = 300
_END_DPP_MS = 2970
_TRIM_RISE_TIME_MS = 150
_SURVIVAL_CHECK_AFTER_MS = 2900

_SCAN_SIDE = "NEG"
_OFFSETS_NEG = np.array([-1.0e-3, -0.75e-3, -0.5e-3, -0.25e-3])
_OFFSETS_POS = np.array([0.25e-3, 0.5e-3, 0.75e-3, 1.0e-3])

_APPLY_TRIMS = False
_RESTORE_REFERENCE = False
_SAVE_RADIAL_STEERING = False


# ============================
# =========  SCRIPT  =========
# ============================

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
variables = {
    "first_callback": True,
    "repetition": 1,
    "offset_id": 0,
    "finished": False,
    "t_settings_set": pd.Timestamp.now(tz="UTC"),
}

offsets = _OFFSETS_NEG if _SCAN_SIDE.upper() == "NEG" else _OFFSETS_POS
reference_dpp = copy.deepcopy(japc.getParam(_DPP))


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def current_offset():
    return float(offsets[variables["offset_id"]])


def current_repetition():
    return variables["repetition"]


def side_folder():
    return f"{shortname}_{_SCAN_SIDE.upper()}"


def offset_label(value):
    return f"{value:+.6e}".replace("+", "plus_").replace("-", "minus_")


def result_path():
    return Path(f"results/{side_folder()}/{offset_label(current_offset())}/id{current_repetition()}.json")


def result_exists():
    return result_path().exists()


def store_result(payload):
    path = result_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fp:
        json.dump(payload, fp, indent=2, default=json_default)
    print_log(f"Stored {path}")


def advance_scan():
    if variables["repetition"] < repetitions:
        variables["repetition"] += 1
        return False

    variables["repetition"] = 1
    if variables["offset_id"] < len(offsets) - 1:
        variables["offset_id"] += 1
        return True

    variables["finished"] = True
    return False


def skip_existing_results():
    while not variables["finished"] and result_exists():
        offset_changed = advance_scan()
        if variables["finished"]:
            return False
        if offset_changed:
            return True
    return False


def interpolate_lsa_value(function_data, query_x):
    x = np.asarray(function_data["X"], dtype=float)
    y = np.asarray(function_data["Y"], dtype=float)
    return float(np.interp(query_x, x, y))


def build_dpp_function(target_value):
    raw = copy.deepcopy(reference_dpp)
    function_data = raw["JAPC_FUNCTION"] if "JAPC_FUNCTION" in raw else raw["value"]["JAPC_FUNCTION"]

    x = np.asarray(function_data["X"], dtype=float)
    query_start = _CYCLE_OFFSET_MS + _START_DPP_MS - _TRIM_RISE_TIME_MS
    query_plateau_start = _CYCLE_OFFSET_MS + _START_DPP_MS
    query_plateau_end = _CYCLE_OFFSET_MS + _END_DPP_MS
    query_end = _CYCLE_OFFSET_MS + _END_DPP_MS + _TRIM_RISE_TIME_MS

    y_start = interpolate_lsa_value(function_data, query_start)
    y_end = interpolate_lsa_value(function_data, query_end)

    function_data["X"] = np.asarray(
        [x[0], query_start, query_plateau_start, query_plateau_end, query_end, x[-1]],
        dtype=float,
    )
    function_data["Y"] = np.asarray(
        [y_start, y_start, target_value, target_value, y_end, y_end],
        dtype=float,
    )
    return raw


def apply_offset(target_value):
    # This is the only place where the script writes settings back to JAPC.
    if not _APPLY_TRIMS:
        print_log(f"Dry-run: would set {_DPP} to {target_value:+.6e}")
        variables["t_settings_set"] = pd.Timestamp.now(tz="UTC")
        return
    japc.setParam(_DPP, build_dpp_function(target_value))
    variables["t_settings_set"] = pd.Timestamp.now(tz="UTC")
    print_log(f"Applied {_DPP} = {target_value:+.6e}")


def get_cycle_value(raw, cycle_start_ms=_START_DPP_MS):
    return bbq.get_lsa_time_value(
        raw,
        cycle_offset_ms=_CYCLE_OFFSET_MS,
        cycle_start_ms=cycle_start_ms,
    )


def beam_survives(bct_result):
    return beam_injected(bct_result, t_after_ms=_SURVIVAL_CHECK_AFTER_MS)


def get_optional_cycle_value(device_name):
    if not device_name:
        return None
    raw = japc.getParam(device_name)
    return {
        "device": device_name,
        "value": get_cycle_value(raw),
        "raw": raw,
    }


def acquire_snapshot(bbq_value, bbq_header):
    # BBQ is the anchor subscription. Everything else is fetched when BBQ arrives.
    bct_value = japc.getParam(_BCT)
    dpp_raw = japc.getParam(_DPP)
    revfreq_raw = japc.getParam(_REVFREQ)
    radial_raw = japc.getParam(_RADIAL) if _SAVE_RADIAL_STEERING else None
    qh_ref = get_optional_cycle_value(_QH_REF)
    qv_ref = get_optional_cycle_value(_QV_REF)

    snapshot = {
        "studyname": studydesc,
        "shortname": shortname,
        "cycle_name": _CYCLE_NAME,
        "selector": _SPS_USER,
        "side": _SCAN_SIDE.upper(),
        "offset_target": current_offset(),
        "repetition": current_repetition(),
        "start_dpp_ms": _START_DPP_MS,
        "end_dpp_ms": _END_DPP_MS,
        "trim_rise_time_ms": _TRIM_RISE_TIME_MS,
        "survival_check_after_ms": _SURVIVAL_CHECK_AFTER_MS,
        "injection_time_ms": INJECTION_TIME_MS,
        "saved_at_utc": f"{pd.Timestamp.now(tz='UTC')}".split(".")[0],
        "header": bbq_header,
        "derived": {
            "measured_dp_over_p": get_cycle_value(dpp_raw),
        },
        "raw": {
            "bbq": {"header": bbq_header, "value": bbq_value},
            "bct": bct_value,
            "dp_over_p": dpp_raw,
            "revfreq": revfreq_raw,
        },
    }

    if radial_raw is not None:
        snapshot["derived"]["measured_radial_steering"] = get_cycle_value(radial_raw)
        snapshot["raw"]["radial_steering"] = radial_raw

    if qh_ref is not None:
        snapshot["derived"]["starting_qh"] = qh_ref["value"]
        snapshot["raw"]["qh_reference"] = qh_ref["raw"]

    if qv_ref is not None:
        snapshot["derived"]["starting_qv"] = qv_ref["value"]
        snapshot["raw"]["qv_reference"] = qv_ref["raw"]

    return snapshot


def on_bbq(_param, bbq_value, bbq_header):
    if variables["finished"]:
        return

    # First BBQ callback is only used to place the first dp/p setting.
    if variables["first_callback"]:
        apply_offset(current_offset())
        variables["first_callback"] = False
        return

    # Once BBQ arrives, fetch BCT to verify the machine state for the same cycle.
    bct_value = japc.getParam(_BCT)
    cycle_time = bbq_header["cycleStamp"]
    print_log(
        f"Cycle {cycle_time}: offset {current_offset():+.6e} rep {current_repetition()}",
        end="",
    )

    if not beam_injected(bct_value, t_before_ms=_START_DPP_MS):
        print_log(" -> ERROR. No beam before measurement window. Trying again.")
        return

    if not beam_survives(bct_value):
        print_log(" -> ERROR. Beam lost before the end of the measurement window. Trying again.")
        return

    if variables["t_settings_set"] + pd.Timedelta(milliseconds=1500) > pd.Timestamp(cycle_time):
        print_log(f" -> ERROR. Settings were not set in time ({variables['t_settings_set']}). Trying again.")
        return

    payload = acquire_snapshot(bbq_value, bbq_header)
    store_result(payload)
    print_log(" -> OK")

    # Advance repetition first. Only move to the next offset after 3 saved files.
    offset_changed = advance_scan()
    offset_changed = skip_existing_results() or offset_changed

    if variables["finished"]:
        if _RESTORE_REFERENCE and _APPLY_TRIMS:
            japc.setParam(_DPP, reference_dpp)
        print_log("Cool, we are done!")
        japc.stopSubscriptions()
        return

    if offset_changed:
        apply_offset(current_offset())


japc.rbacLogin()
japc.subscribeParam(_BBQ, on_bbq, getHeader=True)

print_log()
print_log(
    f"{this_time}: Starting run with name '{shortname}' on side '{_SCAN_SIDE.upper()}' "
    f"and offsets {list(offsets)}"
)
print_log("                     Subscription anchor: BBQ.")
print_log("                     For each BBQ acquisition, the script fetches BCT and saves the full raw payloads.")
print_log("                     Beam must be present before the measurement window and still present at the end.")
print_log("                     Run this script once for NEG offsets and once for POS offsets.")
if not _APPLY_TRIMS:
    print_log("                     Dry-run mode is enabled. No dp/p trims will be sent.")
japc.startSubscriptions()
