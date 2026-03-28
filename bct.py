from __future__ import annotations

import numpy as np

INTENSITY_THRESHOLD = 5e9
INJECTION_TIME_MS = 1015


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
    if t_before_ms is not None:
        intensity = intensity[times < t_before_ms + INJECTION_TIME_MS]
    if t_after_ms is not None:
        intensity = intensity[times > t_after_ms + INJECTION_TIME_MS]
    return intensity.size > 0 and float(np.max(intensity)) > threshold


def beam_killed(bct_result, t_before_ms=None, t_after_ms=None, threshold=INTENSITY_THRESHOLD):
    intensity = total_intensity(bct_result)
    times = time_ms(bct_result)
    if t_before_ms is not None:
        intensity = intensity[times < t_before_ms + INJECTION_TIME_MS]
    if t_after_ms is not None:
        intensity = intensity[times > t_after_ms + INJECTION_TIME_MS]
    return intensity.size > 0 and float(np.max(intensity)) < threshold
