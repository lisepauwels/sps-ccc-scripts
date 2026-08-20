from __future__ import annotations

import numpy as np
from general import store_data

INTENSITY_THRESHOLD = 5e9
INJECTION_TIME_MS = 1015

_BCT = "SPS.BCTDC24.51454/Acquisition"

def log_bct(name, data, header):
   cycle_time = header['cycleStamp']
   store_data(f"rawdata/BCT_data_{cycle_time}", data, header=header)

def subscribe_bct(japc):
    japc.subscribeParam(_BCT, log_bct, getHeader=True)


def total_intensity(bct_result):
    unit = 10 ** bct_result["totalIntensity_unitExponent"]
    return unit * np.asarray(bct_result["totalIntensity"])


def time_ms(bct_result):
    factor = 10 ** bct_result.get("measStamp_unitExponent", 0)
    unit = bct_result["measStamp_units"]
    stamp = np.asarray(bct_result["measStamp"]) * factor
    if unit == "ms":
        return INJECTION_TIME_MS + stamp
    if unit == "s":
        return INJECTION_TIME_MS + 1000.0 * stamp
    if unit == "us":
        return INJECTION_TIME_MS + stamp / 1.0e3
    if unit == "ns":
        return INJECTION_TIME_MS + stamp / 1.0e6
    raise ValueError(f"Unknown BCT time unit: {unit}")


def beam_injected(bct_result, t_before_ms=None, t_after_ms=None, threshold=INTENSITY_THRESHOLD):
    if "beamDetected" in bct_result and not bct_result["beamDetected"]:
        return False
    intensity = total_intensity(bct_result)
    times = time_ms(bct_result)
    mask = np.ones(len(times), dtype=bool)
    if t_before_ms is not None:
        mask &= times < t_before_ms + INJECTION_TIME_MS
    if t_after_ms is not None:
        mask &= times > t_after_ms + INJECTION_TIME_MS
    return intensity.size > 0 and float(np.max(intensity[mask])) > threshold


def beam_killed(bct_result, t_before_ms=None, t_after_ms=None, threshold=INTENSITY_THRESHOLD):
    intensity = total_intensity(bct_result)
    times = time_ms(bct_result)
    mask = np.ones(len(times), dtype=bool)
    if t_before_ms is not None:
        mask &= times < t_before_ms + INJECTION_TIME_MS
    if t_after_ms is not None:
        mask &= times > t_after_ms + INJECTION_TIME_MS
    return intensity.size > 0 and float(np.max(intensity[mask])) < threshold


def beam_alive(bct_result, t_before_ms=None, t_after_ms=None, threshold=INTENSITY_THRESHOLD,
               threshold_percent=None):
    intensity = total_intensity(bct_result)
    times = time_ms(bct_result)
    mask = np.ones(len(times), dtype=bool)
    if threshold_percent is not None:
        mask0 = (times < 500 + INJECTION_TIME_MS) & (times > 100 + INJECTION_TIME_MS)
        intensity_0 = intensity[mask0].mean()
    if t_before_ms is not None:
        mask &= times < t_before_ms + INJECTION_TIME_MS
    if t_after_ms is not None:
        mask &= times > t_after_ms + INJECTION_TIME_MS
    if threshold_percent is not None:
        return intensity.size > 0 and np.all(intensity[mask] > threshold_percent*intensity_0)
    else:
        return intensity.size > 0 and np.all(intensity[mask] > threshold)
