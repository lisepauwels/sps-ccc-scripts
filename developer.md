# Developer Notes

## Goal

Build a CCC workflow to:

- scan `dp/p` offsets, one side at a time
- save full raw BBQ and BCT data for each successful repetition
- later extract and plot tunes from the kicked BBQ signal
- save and live-plot BLM loss maps during tune chirp or radial-steering scans

## Current files

- `run_dpp_scan.py`
  Main save script.
  Subscription anchor is `SPS.BQ.CONT/ContinuousAcquisition`.
  On each BBQ callback it fetches `BCT`, `DpOverPOffset`, and `RevFreq`, validates beam presence, and saves one JSON file.

- `live_tune_monitor.py`
  Optional live tune monitor.
  Uses explicit time windows for horizontal and vertical kicks.

- `inspect_ccc_devices.py`
  Helper for SWAN / CCC exploration.
  Dumps device structures into `inspection/` so we can confirm available fields and correct device names.

- `loss_map_monitor.py`
  BLM scaffold for live loss-map plotting and raw waveform saving.
  Uses the BLM waveform arrays from ExpertAcquisition and selects the plotting time from the maximum total loss inside a configurable cycle-time window.
  Plotting is logarithmic, and monitors with no usable signal are shown as blue points at the plot floor so they remain visible on the map.

## Save logic

- first BBQ callback applies the first `dp/p` target
- later callbacks:
  - fetch BCT
  - require beam before the measurement window
  - require beam still present near the end of the cycle
  - save one JSON for the current repetition
- 3 saved repetitions per offset
- one side per run: `NEG` or `POS`

## Current assumptions

- injection time is `1015 ms`
- cycle length is around `3 s`
- `DpOverPOffset` is the knob to scan
- `SPS.BQ.CONT/ContinuousAcquisition` is the current BBQ source

## What is still needed from CCC / SWAN

1. Exact writeback format for `SpsLowLevelRF/DpOverPOffset`
   Current code has a placeholder plateau builder, but `_APPLY_TRIMS` must stay `False` until this is confirmed.

2. Confirmation that `SPS.BQ.CONT/ContinuousAcquisition` is the right BBQ source
   We need to know whether this is sufficient, or whether there is a better kick-based BBQ acquisition to use instead.

3. Candidate tune-reference device names
   The save script can optionally store starting `QH` and `QV`, but `_QH_REF` and `_QV_REF` are still `None`.

4. Final timing
   Need final values for:
   - start of `dp/p` action
   - end of `dp/p` action
   - beam survival check time
   - live kick windows for `H` and `V`
   - BLM save window for chirp / radial-steering scans

5. BLM acquisition prefixes
   We now know the param pattern on SWAN:
   - `BLRSPS_<pos>:ExpertAcquisition:beamLossMeasurements_gray`
   - `BLRSPS_<pos>:ExpertAcquisition:beamLossMeasurementTimes_ms`
   - `BLRSPS_<pos>:ExpertAcquisition:channelNames`
   But we still need the list of actual `<pos>` prefixes to monitor.

## Notes on radial steering

Radial steering is optional now.
It is not saved unless `_SAVE_RADIAL_STEERING = True`.
Reason: it is downstream of `dp/p`, so it is useful as context only, not as a core scan variable.

## Notes on tunes

- `QPH/QPV` are chromaticity-related and should not be used as tune references
- live tune extraction should use:
  - explicit kick windows
  - FFT peak candidates
  - optional machine tune references as priors, once the correct devices are known

## Immediate next step

Run `inspect_ccc_devices.py` on SWAN or in the CCC and inspect:

- available BBQ acquisitions
- the exact shape of `SpsLowLevelRF/DpOverPOffset`
- the actual tune-reference devices
- the actual `BLRSPS_<pos>:ExpertAcquisition` prefixes to use for BLMs
