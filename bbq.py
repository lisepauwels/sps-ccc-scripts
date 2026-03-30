from __future__ import annotations

import math
import numpy as np


from general import store_data

_BBQ = "SPS.BQ.KICKED/Acquisition"
_BBQ_FULL = "SPS.BQ.KICKED/ContinuousAcquisition"


def log_bbq(name, data, header):
   cycle_time = header['cycleStamp']
   store_data(f"rawdata/BBQ_data_{cycle_time}", data, header=header)

def log_bbq_full(name, data, header):
   cycle_time = header['cycleStamp']
   store_data(f"rawdata/BBQ_full_data_{cycle_time}", data, header=header)

def subscribe_bbq_kicked(japc):
    japc.subscribeParam(_BBQ, log_bbq, getHeader=True)

def subscribe_bbq_kicked_full(japc):
    japc.subscribeParam(_BBQ_FULL, log_bbq_full, getHeader=True)








def _value_dict(raw):
    return raw["value"] if isinstance(raw, dict) and "value" in raw else raw


def get_lsa_time_value(raw, cycle_offset_ms=0.0, cycle_start_ms=0.0):
    data = _value_dict(raw)["JAPC_FUNCTION"]
    x = np.asarray(data["X"], dtype=float) - cycle_offset_ms
    y = np.asarray(data["Y"], dtype=float)
    valid = y[x >= cycle_start_ms]
    if valid.size == 0:
        raise ValueError("No LSA point found after requested cycle start.")
    return float(valid[0])


def turn_time_s(bbq_raw):
    data = _value_dict(bbq_raw)
    ms_tags = np.asarray(data["msTags"], dtype=int)
    frev = np.asarray(data["frevFreq"], dtype=float)
    n_turns = len(data["rawDataH"])
    dt = []
    for turns, freq in zip(ms_tags, frev):
        dt.extend([1.0 / freq] * max(0, turns - len(dt)))
    if not dt:
        raise ValueError("BBQ data does not contain revolution frequency information.")
    dt.extend([dt[-1]] * max(0, n_turns - len(dt)))
    return np.cumsum(np.asarray(dt[:n_turns]))


def plane_signal(bbq_raw, plane):
    data = _value_dict(bbq_raw)
    plane = plane.upper()
    if plane not in {"H", "V"}:
        raise ValueError(f"Unsupported plane: {plane}")
    return np.asarray(data[f"rawData{plane}"], dtype=float)


def locate_excited_window(signal, min_turn=0, rms_window=256, analysis_turns=4096):
    signal = np.asarray(signal, dtype=float)
    if signal.size <= analysis_turns:
        return 0, signal.size
    rms_window = max(32, min(rms_window, signal.size // 8))
    power = signal ** 2
    kernel = np.ones(rms_window) / rms_window
    rolling_rms = np.sqrt(np.convolve(power, kernel, mode="same"))
    start_hint = int(np.argmax(rolling_rms[min_turn:]) + min_turn)
    start = max(min_turn, start_hint - rms_window // 2)
    stop = min(signal.size, start + analysis_turns)
    start = max(min_turn, stop - analysis_turns)
    return int(start), int(stop)


def _candidate_peaks(spectrum):
    if spectrum.size < 3:
        return np.arange(spectrum.size)
    mask = (spectrum[1:-1] >= spectrum[:-2]) & (spectrum[1:-1] >= spectrum[2:])
    indices = np.nonzero(mask)[0] + 1
    if indices.size == 0:
        return np.array([int(np.argmax(spectrum))])
    return indices


def estimate_tune_fft(
    signal,
    tune_min=0.05,
    tune_max=0.5,
    expected_tune=None,
    expected_tolerance=0.03,
    n_candidates=5,
):
    samples = np.asarray(signal, dtype=float)
    samples = samples - np.mean(samples)
    if samples.size < 32:
        return {"selected_tune": math.nan, "candidates": [], "fft_size": int(samples.size)}

    window = np.hanning(samples.size)
    spectrum = np.abs(np.fft.rfft(samples * window))
    freq = np.fft.rfftfreq(samples.size, d=1.0)
    band = (freq >= tune_min) & (freq <= tune_max)
    if not np.any(band):
        return {"selected_tune": math.nan, "candidates": [], "fft_size": int(samples.size)}

    band_freq = freq[band]
    band_spectrum = spectrum[band]
    peak_indices = _candidate_peaks(band_spectrum)
    order = peak_indices[np.argsort(band_spectrum[peak_indices])[::-1]]

    candidates = []
    for idx in order[:n_candidates]:
        candidates.append(
            {
                "tune": float(band_freq[idx]),
                "amplitude": float(band_spectrum[idx]),
            }
        )

    selected = math.nan
    if candidates:
        if expected_tune is not None:
            close = [
                candidate
                for candidate in candidates
                if abs(candidate["tune"] - expected_tune) <= expected_tolerance
            ]
            selected = close[0]["tune"] if close else candidates[0]["tune"]
        else:
            selected = candidates[0]["tune"]

    return {
        "selected_tune": float(selected),
        "candidates": candidates,
        "fft_size": int(samples.size),
    }


def estimate_tune_from_bbq(
    bbq_raw,
    plane,
    expected_tune=None,
    expected_tolerance=0.03,
    min_turn=0,
    rms_window=256,
    analysis_turns=4096,
    tune_min=0.05,
    tune_max=0.5,
):
    signal = plane_signal(bbq_raw, plane)
    start, stop = locate_excited_window(
        signal,
        min_turn=min_turn,
        rms_window=rms_window,
        analysis_turns=analysis_turns,
    )
    result = estimate_tune_fft(
        signal[start:stop],
        tune_min=tune_min,
        tune_max=tune_max,
        expected_tune=expected_tune,
        expected_tolerance=expected_tolerance,
    )
    result["window_start_turn"] = int(start)
    result["window_stop_turn"] = int(stop)
    return result
