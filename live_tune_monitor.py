from __future__ import annotations

import matplotlib.pyplot as plt
import pyjapc

import bbq


_SPS_USER = "SPS.USER.MD2"
_BBQ = "SPS.BQ.CONT/ContinuousAcquisition"
_DPP = "SpsLowLevelRF/DpOverPOffset"
_CYCLE_OFFSET_MS = 1015
_CYCLE_START_MS = 150

_EXPECTED_TUNE_H = None
_EXPECTED_TUNE_V = None
_EXPECTED_TOLERANCE = 0.03

# Set these windows to the actual kick timing for each plane.
_WINDOW_H_START_MS = 350
_WINDOW_H_STOP_MS = 550
_WINDOW_V_START_MS = 650
_WINDOW_V_STOP_MS = 850


japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
plt.ion()
figure, axes = plt.subplots(1, 2, figsize=(10, 4))
history = {"dp": [], "qh": [], "qv": []}


def extract_window_from_ms(bbq_raw, plane, start_ms, stop_ms):
    times_ms = 1.0e3 * bbq.turn_time_s(bbq_raw)
    signal = bbq.plane_signal(bbq_raw, plane)
    mask = (times_ms >= start_ms) & (times_ms <= stop_ms)
    return signal[mask]


def update_plot(_name, bbq_raw, _header):
    dpp_raw = japc.getParam(_DPP)
    measured_dpp = bbq.get_lsa_time_value(
        dpp_raw,
        cycle_offset_ms=_CYCLE_OFFSET_MS,
        cycle_start_ms=_CYCLE_START_MS,
    )
    signal_h = extract_window_from_ms(bbq_raw, "H", _WINDOW_H_START_MS, _WINDOW_H_STOP_MS)
    signal_v = extract_window_from_ms(bbq_raw, "V", _WINDOW_V_START_MS, _WINDOW_V_STOP_MS)
    tune_h = bbq.estimate_tune_fft(
        signal_h,
        expected_tune=_EXPECTED_TUNE_H,
        expected_tolerance=_EXPECTED_TOLERANCE,
    )["selected_tune"]
    tune_v = bbq.estimate_tune_fft(
        signal_v,
        expected_tune=_EXPECTED_TUNE_V,
        expected_tolerance=_EXPECTED_TOLERANCE,
    )["selected_tune"]

    history["dp"].append(measured_dpp)
    history["qh"].append(tune_h)
    history["qv"].append(tune_v)

    axes[0].cla()
    axes[1].cla()
    axes[0].scatter(history["dp"], history["qh"])
    axes[1].scatter(history["dp"], history["qv"])
    axes[0].set_xlabel("dp/p")
    axes[1].set_xlabel("dp/p")
    axes[0].set_ylabel("QH")
    axes[1].set_ylabel("QV")
    axes[0].set_title("Live horizontal tune")
    axes[1].set_title("Live vertical tune")
    figure.tight_layout()
    figure.canvas.draw()
    figure.canvas.flush_events()


japc.subscribeParam(_BBQ, update_plot, getHeader=True)
japc.startSubscriptions()
