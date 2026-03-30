from __future__ import annotations

from queue import Empty, Queue

import matplotlib.pyplot as plt
import numpy as np
import pyjapc

import bbq


_SPS_USER = "SPS.USER.MD2"
_BBQ = "SPS.BQ.KICKED/Acquisition"
_REFRESH_S = 0.1
_TUNE_MIN = 0.05
_TUNE_MAX = 0.5
_EXPECTED_TUNE_H = None
_EXPECTED_TUNE_V = None
_EXPECTED_TOLERANCE = 0.03

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
updates = Queue(maxsize=1)


def update_plot(name, data, header):
    raw_h = np.asarray(data["rawDataH"], dtype=float)
    raw_v = np.asarray(data["rawDataV"], dtype=float)
    fft_h = bbq.estimate_tune_fft(
        raw_h,
        tune_min=_TUNE_MIN,
        tune_max=_TUNE_MAX,
        expected_tune=_EXPECTED_TUNE_H,
        expected_tolerance=_EXPECTED_TOLERANCE,
    )
    fft_v = bbq.estimate_tune_fft(
        raw_v,
        tune_min=_TUNE_MIN,
        tune_max=_TUNE_MAX,
        expected_tune=_EXPECTED_TUNE_V,
        expected_tolerance=_EXPECTED_TOLERANCE,
    )
    freq_h, spec_h = bbq.tune_spectrum(raw_h, tune_min=_TUNE_MIN, tune_max=_TUNE_MAX)
    freq_v, spec_v = bbq.tune_spectrum(raw_v, tune_min=_TUNE_MIN, tune_max=_TUNE_MAX)

    payload = {
        "rawDataH": raw_h,
        "rawDataV": raw_v,
        "freqH": freq_h,
        "freqV": freq_v,
        "specH": spec_h,
        "specV": spec_v,
        "tuneH": fft_h["selected_tune"],
        "tuneV": fft_v["selected_tune"],
    }

    while not updates.empty():
        try:
            updates.get_nowait()
        except Empty:
            break

    updates.put_nowait(payload)


def main():
    plt.ion()
    figure_pos, axes_pos = plt.subplots(1, 2, num="BBQ Positions", figsize=(10, 4))
    figure_fft, axes_fft = plt.subplots(1, 2, num="BBQ Spectra", figsize=(10, 4))
    figure_est, axes_est = plt.subplots(1, 2, num="Tune Estimates", figsize=(10, 4))

    latest = None
    history_h = []
    history_v = []

    japc.rbacLogin()
    japc.subscribeParam(_BBQ, update_plot, getHeader=True)
    japc.startSubscriptions()

    try:
        while True:
            try:
                while True:
                    latest = updates.get_nowait()
            except Empty:
                pass

            if latest is not None:
                history_h.append(latest["tuneH"])
                history_v.append(latest["tuneV"])

                axes_pos[0].cla()
                axes_pos[1].cla()
                axes_pos[0].plot(latest["rawDataH"])
                axes_pos[1].plot(latest["rawDataV"])
                axes_pos[0].set_title("Kicked BBQ H")
                axes_pos[1].set_title("Kicked BBQ V")
                axes_pos[0].set_ylabel("Position [a.u.]")
                axes_pos[0].set_xlabel("Turn")
                axes_pos[1].set_xlabel("Turn")
                figure_pos.tight_layout()
                figure_pos.canvas.draw_idle()

                axes_fft[0].cla()
                axes_fft[1].cla()
                axes_fft[0].plot(latest["freqH"], latest["specH"], color="black")
                axes_fft[1].plot(latest["freqV"], latest["specV"], color="black")
                if np.isfinite(latest["tuneH"]):
                    axes_fft[0].axvline(latest["tuneH"], color="red", linewidth=2)
                if np.isfinite(latest["tuneV"]):
                    axes_fft[1].axvline(latest["tuneV"], color="red", linewidth=2)
                axes_fft[0].set_title("FFT H")
                axes_fft[1].set_title("FFT V")
                axes_fft[0].set_xlabel("Tune")
                axes_fft[1].set_xlabel("Tune")
                axes_fft[0].set_ylabel("Amplitude")
                figure_fft.tight_layout()
                figure_fft.canvas.draw_idle()

                axes_est[0].cla()
                axes_est[1].cla()
                axes_est[0].plot(history_h, color="tab:blue")
                axes_est[1].plot(history_v, color="tab:orange")
                axes_est[0].set_title("Estimated QH")
                axes_est[1].set_title("Estimated QV")
                axes_est[0].set_xlabel("Acquisition index")
                axes_est[1].set_xlabel("Acquisition index")
                axes_est[0].set_ylabel("Tune")
                figure_est.tight_layout()
                figure_est.canvas.draw_idle()

            plt.pause(_REFRESH_S)
    finally:
        japc.stopSubscriptions()


if __name__ == "__main__":
    main()
