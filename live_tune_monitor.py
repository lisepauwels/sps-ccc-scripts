from __future__ import annotations

from queue import Empty, Queue

import matplotlib.pyplot as plt
import pyjapc


_SPS_USER = "SPS.USER.MD2"
_BBQ = "SPS.BQ.KICKED/Acquisition"
_REFRESH_S = 0.1

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
updates = Queue(maxsize=1)


def update_plot(name, data, header):
    payload = {
        "rawDataH": data["rawDataH"],
        "rawDataV": data["rawDataV"],
    }

    while not updates.empty():
        try:
            updates.get_nowait()
        except Empty:
            break

    updates.put_nowait(payload)


def main():
    plt.ion()
    figure, axes = plt.subplots(1, 2, figsize=(10, 4))

    latest = None

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
                axes[0].cla()
                axes[1].cla()
                axes[0].plot(latest["rawDataH"])
                axes[1].plot(latest["rawDataV"])
                axes[0].set_title("Kicked BBQ H")
                axes[1].set_title("Kicked BBQ V")
                figure.tight_layout()
                figure.canvas.draw_idle()

            plt.pause(_REFRESH_S)
    finally:
        japc.stopSubscriptions()


if __name__ == "__main__":
    main()
