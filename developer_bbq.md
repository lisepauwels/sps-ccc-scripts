# Developer Notes For BBQ / BCT / DPP

This file documents the existing BBQ, BCT, DPP, and scan scripts so they can be debugged from the CCC without re-reading all source files.

## Scope

This note covers:

- `bbq.py`
- `bct.py`
- `dpp.py`
- `general.py`
- `live_tune_monitor.py`
- `run_dpp_scan.py`

For the loss-map workflow, see [developer_lossmaps.md](/Users/lisepauwels/phd/code/sps-ccc-scripts/developer_lossmaps.md).

## High-Level Workflow

The `dp/p` scan path is:

1. `run_dpp_scan.py` subscribes to the BBQ acquisition.
2. On the first callback it applies the first `dp/p` setting.
3. On later callbacks it fetches BCT and machine settings for the same cycle.
4. It rejects cycles with no beam before the measurement window.
5. It saves one result per successful repetition.
6. It advances repetition first, then offset.

The live tune monitor path is separate:

1. `live_tune_monitor.py` subscribes to kicked BBQ.
2. Each acquisition is converted into horizontal and vertical spectra.
3. A tune estimate is computed per plane.
4. The latest raw signals, spectra, and tune histories are plotted live.

## `general.py`

This file provides the shared JSON save logic for the BBQ workflow.

Functions:

- `store_data(name, data, meta={}, header=None)`
  Writes `../sps-measurements/<name>.json`.
  The JSON structure is:
  - top-level metadata fields from `meta`
  - `header`
  - `data`

  It also cleans some heavy fields before writing.

- `result_exists(name=None)`
  Checks whether a JSON save already exists.

- `clean_bbq_data(result)`
  Removes:
  - `rawDataQ`
  - `exDataH`
  - `exDataV`

- `clean_bct_data(result)`
  Removes:
  - `bTrain`
  - `range1Data`
  - `range3Data`

This is the reference save layout that the loss-map JSON mirror now follows conceptually.

## `bct.py`

This file contains helper functions for beam-presence checks.

Constants:

- `INTENSITY_THRESHOLD = 5e9`
- `INJECTION_TIME_MS = 1015`

Functions:

- `total_intensity(bct_result)`
  Converts the BCT total intensity array into physical units using the unit exponent.

- `time_ms(bct_result)`
  Converts the BCT measurement stamp array into milliseconds in the cycle.
  It supports `ms`, `s`, `us`, and `ns`.
  It offsets everything by `INJECTION_TIME_MS`.

- `beam_injected(bct_result, t_before_ms=None, t_after_ms=None, threshold=INTENSITY_THRESHOLD)`
  Returns `True` if beam is detected and the max intensity in the selected time slice is above the threshold.

- `beam_killed(...)`
  Inverse-style helper.
  Returns `True` if the max intensity in the selected time slice is below the threshold.

If scan cycles are unexpectedly rejected, check this file first.

## `bbq.py`

This file contains both save helpers and tune-extraction helpers.

Acquisition constants:

- `_BBQ = "SPS.BQ.KICKED/Acquisition"`
- `_BBQ_FULL = "SPS.BQ.KICKED/ContinuousAcquisition"`

Save helpers:

- `log_bbq(name, data, header)`
  Saves kicked BBQ data through `general.store_data(...)`.

- `log_bbq_full(name, data, header)`
  Saves continuous BBQ data through `general.store_data(...)`.

- `subscribe_bbq_kicked(japc)`
  Subscribes to `_BBQ` with `getHeader=True`.

- `subscribe_bbq_kicked_full(japc)`
  Subscribes to `_BBQ_FULL` with `getHeader=True`.

Analysis helpers:

- `_value_dict(raw)`
  Unwraps JAPC dictionaries that contain a `value` field.

- `get_lsa_time_value(raw, cycle_offset_ms=0.0, cycle_start_ms=0.0)`
  Reads an LSA JAPC function and returns the first value after the requested cycle start.

- `turn_time_s(bbq_raw)`
  Builds a turn-by-turn time axis from revolution frequency information.

- `plane_signal(bbq_raw, plane)`
  Returns `rawDataH` or `rawDataV`.

- `locate_excited_window(signal, min_turn=0, rms_window=256, analysis_turns=4096)`
  Uses rolling RMS to find the most excited part of the signal.

- `_candidate_peaks(spectrum)`
  Finds local peaks in an FFT spectrum.

- `estimate_tune_fft(...)`
  Computes a tune estimate from FFT amplitude peaks inside a tune band.

- `tune_spectrum(signal, tune_min=0.05, tune_max=0.5)`
  Returns the FFT spectrum restricted to the tune band.

- `estimate_tune_from_bbq(...)`
  Full helper:
  1. select `H` or `V`
  2. find excited window
  3. estimate the tune in that window

## `dpp.py`

This file talks to LSA and applies `dp/p` trims.

Global services are initialized at import time:

- `setting_service`
- `context_service`
- `parameter_service`
- `trim_service`

Functions:

- `find_settings(correctors, cycle)`
  Reads context settings for the requested parameters and cycle, returning the setting functions as arrays.

- `dp_offset(offset, t_ms, t_start, t_start_plateau, t_end_plateau, t_end, cycle, description=None)`
  Applies a `dp/p` trim through LSA.

  Important behavior:
  - if `offset == 0`, it prints `No bump requested` and returns
  - it uses `SpsLowLevelRF/DpOverPOffset#value`
  - it creates a plateau-shaped incorporation rule
  - it sends the increment through `trim_service.incorporate(...)`

- `create_incorporation_rule_plateau(...)`
  Builds the plateau rule for incorporation.

If trims do not behave correctly on the CCC, inspect this file and confirm the knob name and timing.

## `live_tune_monitor.py`

This is the optional live plotting script for kicked BBQ.

Important settings:

- `_SPS_USER`
- `_BBQ`
- `_REFRESH_S`
- `_TUNE_MIN`
- `_TUNE_MAX`
- `_EXPECTED_TUNE_H`
- `_EXPECTED_TUNE_V`
- `_EXPECTED_TOLERANCE`

Objects:

- `japc`
  The JAPC client.
- `updates`
  A `Queue(maxsize=1)` used to keep only the newest acquisition for plotting.

Functions:

- `update_plot(name, data, header)`
  Callback for new kicked BBQ acquisitions.

  It:
  1. converts `rawDataH` and `rawDataV` to numpy arrays
  2. estimates tune in both planes with `bbq.estimate_tune_fft(...)`
  3. computes both spectra with `bbq.tune_spectrum(...)`
  4. stores the newest payload in the queue

- `main()`
  Opens three figures:
  - raw positions
  - FFT spectra
  - tune histories

  Then it:
  1. logs in with RBAC
  2. subscribes to kicked BBQ with `getHeader=True`
  3. starts subscriptions
  4. continuously redraws the latest result

If the live plot freezes or lags, first inspect the queue behavior here.

## `run_dpp_scan.py`

This is the main save script for the scan.

Important settings:

- `studydesc`
- `shortname`
- `repetitions`
- `_SPS_USER`
- `_CYCLE_NAME`
- `_BBQ`
- `_BCT`
- `_DPP`
- `_RADIAL`
- `_START_DPP_MS`
- `_END_DPP_MS`
- `_DPP_RISE_TIME_MS`
- `_SURVIVAL_CHECK_AFTER_MS`
- `offsets`

Global objects:

- `japc`
  The JAPC client.
- `variables`
  Runtime state:
  - `first_callback`
  - `repetition`
  - `offset_id`
  - `finished`
  - `t_settings_set`

Functions:

- `current_offset()`
  Returns the active `dp/p` target.

- `current_repetition()`
  Returns the active repetition index.

- `name()`
  Builds the save path:
  `results_chroma/<shortname>/DP<int(offset*1e6)>E-6/id<rep>`

- `advance_scan()`
  Advances repetition first, then offset.
  Sets `finished = True` when the scan is done.

- `skip_existing_results()`
  Skips already-saved JSON outputs.

- `apply_dp_offset(target_value)`
  Calls `dpp.dp_offset(...)` with the configured cycle timing.
  Also stores the time when settings were applied.

- `print_log(*args, **kwargs)`
  Prints to terminal and appends to `run.log`.

- `acquire_snapshot(name, bbq_data, bct_data, header)`
  Fetches additional machine state:
  - `DpOverPOffset`
  - `RadialSteering`

  Then it builds metadata and data payloads and saves them with `general.store_data(...)`.
  It also computes simple `QH` and `QV` estimates from the FFT maxima already present in the BBQ payload.

- `chroma_measurement(_name, data, header)`
  Main callback.

  Logic:
  1. if finished, return
  2. if first callback, apply first offset and return
  3. fetch BCT
  4. verify settings were applied early enough
  5. require beam before the measurement window
  6. if beam is gone before the survival check, log an error and continue
  7. otherwise save the result
  8. advance repetition / offset
  9. if finished, reset `dp/p` to zero and stop subscriptions
  10. if the offset changed, apply the next offset

Final startup block:

1. `japc.rbacLogin()`
2. `bbq.subscribe_bbq_kicked_full(japc)`
3. `japc.subscribeParam(_BBQ, chroma_measurement, getHeader=True)`
4. `japc.startSubscriptions()`

## What To Check On CCC

1. Confirm the exact writeback shape of `SpsLowLevelRF/DpOverPOffset`.
2. Confirm the best BBQ acquisition for the scan and for live monitoring.
3. Confirm the cycle timing values for:
   - start of `dp/p`
   - end of `dp/p`
   - survival check
   - kick windows
4. Confirm BCT intensity thresholds if beam checks are too strict.
5. Confirm the expected tune band if FFT peak picking needs stronger priors.

## Practical Debug Order

1. Check that JAPC subscriptions are firing at all.
2. Check that the callback `header` contains the expected cycle information.
3. Check BCT contents and beam checks.
4. Check whether `dp/p` settings were applied early enough.
5. Check save paths and whether old files are being skipped.
6. Check FFT tune estimates only after acquisition and saving are behaving correctly.

