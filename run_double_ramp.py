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

studydesc = "Qx stepped scan, Qy ramped, across the Qx=Qy coupling resonance, with full BBQ+BCT saving, damper OFF"
shortname = "QX_CROSSING_SCAN_QY_RAMPED_2qx_qy"  # SET: rename to something descriptive of this campaign
repetitions = 3
this_time = f"{pd.Timestamp.now(tz='UTC')}".split(".")[0]

# ============================
# ========= SETTINGS  ========
# ============================

_SPS_USER = "SPS.USER.MD2"
_CYCLE_NAME = "MD_26_L4800_Q20_North_Extraction_2026_V1"  # TODO CONFIRM: same placeholder as sps-resonance-scans

_BBQ = "SPS.BQ.KICKED/Acquisition"
_BCT = "SPS.BCTDC24.51454/Acquisition"

# --- Qx: stepped scan, SET MANUALLY -- make sure this range actually crosses _QX ---
_QX_START = 20.07
_QX_END = 20.13
_QX_STEP = 0.005
_N_QX = int(round((_QX_END - _QX_START) / _QX_STEP)) + 1
qx_values = np.flip(np.round(_QX_START + _QX_STEP * np.arange(_N_QX), 10))

# --- Qy ramp ---
_QY_BEGIN = 20.14
_QY_END = 20.26


# ============================
# =========  SCRIPT  =========
# ============================

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
variables = {
    "first_callback": True,
    "repetition": 0,
    "qx_id": 0,
    "finished": False,
    "t_settings_set": pd.Timestamp.now(tz="UTC"),
}


def current_qx():
    return float(qx_values[variables["qx_id"]])


def current_repetition():
    return variables["repetition"]


def name():
    return f"results_resonances/{shortname}/QX{current_qx():.4f}/id{current_repetition()}"


def advance_scan():
    if variables["repetition"] < repetitions:
        variables["repetition"] += 1
        return False
    variables["repetition"] = 1

    if variables["qx_id"] < len(qx_values) - 1:
        variables["qx_id"] += 1
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


def apply_qx(target_value):
    set_tune_flat(target_value, cycle=_CYCLE_NAME, plane='H',
                  description=f"Qx crossing scan: Qx={target_value:.4f}")
    variables["t_settings_set"] = pd.Timestamp.now(tz="UTC")
    print_log(f"Applied Qx = {target_value:.4f}")


def print_log(*args, **kwargs):
    print(*args, **kwargs)
    with open("run_qx_crossing_scan_qy_ramped.log", "a") as fid:
        print(*args, file=fid, **kwargs)


def acquire_snapshot(name, bbq_data, bct_data, header):
    meta = {
        "studyname": studydesc,
        "shortname": shortname,
        "cycle_name": _CYCLE_NAME,
        "qx_target": current_qx(),
        "qy_target": [_QY_BEGIN, _QY_END],
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


def qx_scan_measurement(_name, data, header):
    if variables["finished"]:
        return

    # First BBQ callback is only used to place the first Qx setting.
    if variables["first_callback"]:
        next_step()
        apply_qx(current_qx())
        variables["first_callback"] = False
        return

    # Once a kicked BBQ acquisition arrives, fetch BCT to verify the machine state for the same cycle.
    bct_data = japc.getParam(_BCT)
    cycle_time = header["cycleStamp"]
    print_log(f"Cycle {cycle_time}: Qx={current_qx():.4f} Qy=[{_QY_BEGIN:.4f}, {_QY_END:.4f}] rep {current_repetition()}", end="")

    if variables["t_settings_set"] + pd.Timedelta(milliseconds=1500) > pd.Timestamp(cycle_time):
        print_log(f" -> ERROR. Settings were not set in time ({variables['t_settings_set']}). Trying again.")
        return

    if not beam_injected(bct_data):
        print_log(" -> ERROR. No beam. Trying again.")
        return


    valH, valV = acquire_snapshot(name(), data, bct_data, header)
    print_log(f" -> OK  (estimate: H={valH:.4f}, V={valV:.4f})")

    # Advance repetition first. Only move to the next Qy after 5 saved files.
    qx_changed = next_step()

    if variables["finished"]:
        print_log("Cool, we are done!")
        japc.stopSubscriptions()
        return

    if qx_changed:
        apply_qx(current_qx())


japc.rbacLogin()
japc.subscribeParam(_BBQ, qx_scan_measurement, getHeader=True)

set_tune_ramp(_QY_BEGIN, _QY_END, cycle=_CYCLE_NAME, plane='V', description=f"Tune scan: Qy ramp {_QY_BEGIN:.4f} -> {_QY_END:.4f}")

print_log()
print_log(f"{this_time}: Starting run with name '{shortname}', "
          f"Qx {qx_values[0]:.4f} -> {qx_values[-1]:.4f} (step {_QX_STEP}), Qy=[{_QY_BEGIN:.4f},{_QY_END:.4f}] ramped. Fire kicks manually.")

japc.startSubscriptions()
