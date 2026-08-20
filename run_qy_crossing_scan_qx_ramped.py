# $ source /user/fvanderv/setup_environment.sh
# $ in ipython: run run_qy_crossing_scan.py
#
# Qx=Qy coupling-resonance crossing scan: set Qx flat once, then step Qy through a list
# of discrete values (crossing Qx at some point), 5 kicks (repetitions) per Qy value.
# Kicks are NOT triggered by this script -- fire them manually; the BBQ kicked
# acquisition (_BBQ) is the anchor subscription that drives the state machine, same
# pattern as run_dpp_scan.py in sps-ccc-scripts. The full BBQ continuous acquisition is
# also saved unconditionally, via bbq.subscribe_bbq_kicked_full (unchanged from
# sps-ccc-scripts/bbq.py).

from __future__ import annotations

import pyjapc
import numpy as np
import pandas as pd

from general import clean_bbq_data, clean_bct_data, store_data, result_exists
from bbq import subscribe_bbq_kicked_full
from bct import beam_injected
from tune_trim import set_tune_flat, set_tune_ramp

studydesc = "Qx ramped, Qy stepped scan, across the Qx=Qy coupling resonance, with full BBQ+BCT saving, damper OFF"
shortname = "QY_CROSSING_SCAN_QX_RAMPED"  # SET: rename to something descriptive of this campaign
repetitions = 3
this_time = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

# ============================
# ========= SETTINGS  ========
# ============================

_SPS_USER = "SPS.USER.MD2"
_CYCLE_NAME = "MD_26_L4800_Q20_North_Extraction_2026_V1"  # TODO CONFIRM: same placeholder as sps-resonance-scans

_BBQ = "SPS.BQ.KICKED/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"

# --- Qx ramp ---
_QX_BEGIN = 20.04
_QX_END = 20.2

# --- Qy: stepped scan, SET MANUALLY -- make sure this range actually crosses _QX ---
_QY_START = 20.105
_QY_END = 20.155
_QY_STEP = 0.005
_N_QY = int(round((_QY_END - _QY_START) / _QY_STEP)) + 1
qy_values = np.flip(np.round(_QY_START + _QY_STEP * np.arange(_N_QY), 10))


# ============================
# =========  SCRIPT  =========
# ============================

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
variables = {
    "first_callback": True,
    "repetition": 0,
    "qy_id": 0,
    "finished": False,
    "t_settings_set": pd.Timestamp.now(tz="UTC"),
}


def current_qy():
    return float(qy_values[variables["qy_id"]])


def current_repetition():
    return variables["repetition"]


def name():
    return f"results_resonances/{shortname}/QY{current_qy():.4f}/id{current_repetition()}"


def advance_scan():
    if variables["repetition"] < repetitions:
        variables["repetition"] += 1
        return False
    variables["repetition"] = 1

    if variables["qy_id"] < len(qy_values) - 1:
        variables["qy_id"] += 1
        return True

    variables["finished"] = True
    return False


def next_step():
    changed = advance_scan()
    while not variables["finished"] and result_exists(name()):
        changed2 = advance_scan()
        changed = changed or changed2
        if variables["finished"]:
            return True
    return changed


def apply_qy(target_value):
    set_tune_flat(target_value, cycle=_CYCLE_NAME, plane='V',
                  description=f"Qy crossing scan: Qy={target_value:.4f}")
    variables["t_settings_set"] = pd.Timestamp.now(tz="UTC")
    print_log(f"Applied Qy = {target_value:.4f}")


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run_qy_crossing_scan_qx_ramped.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def acquire_snapshot(name, bbq_data, bct_data, header):
    meta = {
        "studyname": studydesc,
        "shortname": shortname,
        "cycle_name": _CYCLE_NAME,
        "qx_target": [_QX_BEGIN, _QX_END],
        "qy_target": current_qy(),
        "repetition": current_repetition(),
        "saved_at_utc": f"{pd.Timestamp.now(tz='UTC')}".split(".")[0],
    }
    valH = np.arange(0, 1, 1 / len(bbq_data['fftDataH']))[bbq_data['fftDataH'].argmax()] % 0.5
    valV = np.arange(0, 1, 1 / len(bbq_data['fftDataV']))[bbq_data['fftDataV'].argmax()] % 0.5
    data = {
        "tune_H_est": valH,
        "tune_V_est": valV,
        "bbq": clean_bbq_data(bbq_data),
        "bct": clean_bct_data(bct_data),
    }
    store_data(name, data=data, meta=meta, header=header)
    return valH, valV


def qy_scan_measurement(_name, data, header):
    if variables["finished"]:
        return

    # First BBQ callback is only used to place the first Qy setting.
    if variables["first_callback"]:
        next_step()
        apply_qy(current_qy())
        variables["first_callback"] = False
        return

    # Once a kicked BBQ acquisition arrives, fetch BCT to verify the machine state for the same cycle.
    bct_data = japc.getParam(_BCT)
    cycle_time = header["cycleStamp"]
    print_log(f"Cycle {cycle_time}: Qx=[{_QX_BEGIN:.4f}, {_QX_END:.4f}] Qy={current_qy():.4f} rep {current_repetition()}", end="")

    if variables["t_settings_set"] + pd.Timedelta(milliseconds=1500) > pd.Timestamp(cycle_time):
        print_log(f" -> ERROR. Settings were not set in time ({variables['t_settings_set']}). Trying again.")
        return

    if not beam_injected(bct_data):
        print_log(" -> ERROR. No beam. Trying again.")
        return


    valH, valV = acquire_snapshot(name(), data, bct_data, header)
    print_log(f" -> OK  (estimate: H={valH:.4f}, V={valV:.4f})")

    # Advance repetition first. Only move to the next Qy after 5 saved files.
    qy_changed = next_step()

    if variables["finished"]:
        print_log("Cool, we are done!")
        japc.stopSubscriptions()
        return

    if qy_changed:
        apply_qy(current_qy())


japc.rbacLogin()
japc.subscribeParam(_BBQ, qy_scan_measurement, getHeader=True)

set_tune_ramp(_QX_BEGIN, _QX_END, cycle=_CYCLE_NAME, plane='H', description=f"Tune scan: Qx ramp {_QX_BEGIN:.4f} -> {_QX_END:.4f}")

print_log()
print_log(f"{this_time}: Starting run with name '{shortname}', "
          f"Qy {qy_values[0]:.4f} -> {qy_values[-1]:.4f} (step {_QY_STEP}), Qx=[{_QX_BEGIN:.4f},{_QX_END:.4f}] ramped. Fire kicks manually.")

japc.startSubscriptions()
