from __future__ import annotations

import pyjapc
import numpy as np
import pandas as pd

from general import clean_bbq_data, clean_bct_data, store_data, result_exists
from bbq import subscribe_bbq_kicked_full
from bct import INJECTION_TIME_MS, beam_injected
from dpp import dp_offset

studydesc = "dp/p scan with full BBQ and BCT saving"
shortname = "Q_DPP_SCAN_CHROM_0.154_0.3"
repetitions = 5
this_time = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

# ============================
# ========= SETTINGS  ========
# ============================

_SPS_USER = "SPS.USER.MD2"
_CYCLE_NAME = "MD_26_L4800_Q20_North_Extraction_2026_V1"

_BBQ = "SPS.BQ.KICKED/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"
_DPP = "SpsLowLevelRF/DpOverPOffset"
_RADIAL = "SpsLowLevelRF/RadialSteering"

_START_DPP_MS = 1800
_END_DPP_MS = 2880
_DPP_RISE_TIME_MS = 1500
_SURVIVAL_CHECK_AFTER_MS = 2400

offsets = np.array([x for y in np.arange(0, 6.75e-3, 2.5e-4) for x in [-y, y]])[1:]


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

def current_offset():
    return float(offsets[variables["offset_id"]])

def current_repetition():
    return variables["repetition"]

def name():
    return f"{shortname}/{int(current_offset()*1e6)}E-6/id{current_repetition()}"

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
    while not variables["finished"] and result_exists(name()):
        changed = advance_scan()
        if variables["finished"]:
            return False
        if changed:
            return True
    return False

def apply_dp_offset(target_value):
    dp_offset(offset=target_value, t_ms=2000,  # to get roughly into the injection process
              t_start=_START_DPP_MS - _DPP_RISE_TIME_MS,
              t_start_plateau=_START_DPP_MS,
              t_end_plateau=_END_DPP_MS,
              t_end=_END_DPP_MS + 1,
              cycle=_CYCLE_NAME,
              description = f"Chroma measurement: DP={target_value:+.6e}")
    variables["t_settings_set"] = pd.Timestamp.now(tz="UTC")
    print_log(f"Applied {_DPP} = {target_value:+.6e}")

def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run.log", "a") as fid:
        print(*args, file=fid, **kwargs)

def acquire_snapshot(name, bbq_data, bct_data, header):
    # BBQ is the anchor subscription. Everything else is fetched when BBQ arrives.
    dpp_raw = japc.getParam(_DPP)
    radial_raw = japc.getParam(_RADIAL)

    meta = {
        "studyname": studydesc,
        "shortname": shortname,
        "cycle_name": _CYCLE_NAME,
        "offset_target": current_offset(),
        "repetition": current_repetition(),
        "start_dpp_ms": _START_DPP_MS,
        "end_dpp_ms": _END_DPP_MS,
        "dpp_rise_time_ms": _DPP_RISE_TIME_MS,
        "survival_check_after_ms": _SURVIVAL_CHECK_AFTER_MS,
        "injection_time_ms": INJECTION_TIME_MS,
        "saved_at_utc": f"{pd.Timestamp.now(tz='UTC')}".split(".")[0],
    }
    valH = np.arange(0, 1, 1/len(bbq_data['fftDataH']))[bbq_data['fftDataH'].argmax()] % 0.5
    valV = np.arange(0, 1, 1/len(bbq_data['fftDataV']))[bbq_data['fftDataV'].argmax()] % 0.5
    data = {
            "tune_H_est": valH,
            "tune_V_est": valV,
            "bbq": clean_bbq_data(bbq_data),
            "bct": clean_bct_data(bct_data),
            "dp_over_p": dpp_raw,
            "radial_steering": radial_raw
        }
    store_data(name, data=data, meta=meta, header=header)
    return valH, valV


def chroma_measurement(_name, data, header):
    if variables["finished"]:
        return

    # First BBQ callback is only used to place the first dp/p setting.
    if variables["first_callback"]:
        apply_dp_offset(current_offset())
        variables["first_callback"] = False
        return

    # Once BBQ arrives, fetch BCT to verify the machine state for the same cycle.
    bct_data = japc.getParam(_BCT)
    cycle_time = header["cycleStamp"]
    print_log(
        f"Cycle {cycle_time}: offset {current_offset():+.6e} rep {current_repetition()}",
        end="",
    )

    if variables["t_settings_set"] + pd.Timedelta(milliseconds=1500) > pd.Timestamp(cycle_time):
        print_log(f" -> ERROR. Settings were not set in time ({variables['t_settings_set']}). Trying again.")
        return

    if not beam_injected(bct_data, t_before_ms=_START_DPP_MS):
        print_log(" -> ERROR. No beam before measurement window. Trying again.")
        return

    if not beam_injected(bct_data, t_before_ms=_SURVIVAL_CHECK_AFTER_MS):
        print_log(" -> ERROR. Beam lost before the end of the measurement window. Continuing.")
    else:
        valH, valV = acquire_snapshot(name(), data, bct_data, header)
        print_log(f" -> OK  (estimate: H={valH:.4f}, V={valV:.4f})")

    # Advance repetition first. Only move to the next offset after 5 saved files.
    offset_changed = advance_scan()
    offset_changed = skip_existing_results() or offset_changed

    if variables["finished"]:
        apply_dp_offset(0.)
        print_log("Cool, we are done!")
        japc.stopSubscriptions()
        return

    if offset_changed:
        apply_dp_offset(current_offset())


japc.rbacLogin()
subscribe_bbq_kicked_full(japc)
japc.subscribeParam(_BBQ, chroma_measurement, getHeader=True)

print_log()
print_log(f"{this_time}: Starting run with name '{shortname}' and offsets {list(offsets)}")

japc.startSubscriptions()

