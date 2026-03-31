# Developer Notes For Loss Maps
codex resume 019d3f10-2acd-76e2-a572-3ee8e5287547
This file explains the loss-map scripts and helper functions in enough detail to debug them from the CCC.

## Goal

The loss-map workflow is split into three user-facing scripts:

- `loss_map_save.py`
- `loss_map_live_plot.py`
- `loss_map_postprocess.py`

Shared logic lives in:

- `loss_map_common.py`

The old `loss_map_monitor.py` is now only a stub that tells you to use the split scripts.

## High-Level Data Flow

The runtime flow for saving is:

1. Subscribe to the anchor device with `getHeader=True`.
2. When the callback fires, use the callback header as the cycle header.
3. Use the callback `anchor_value` itself as the BCT payload for that exact subscribed cycle.
4. Accept only callbacks from the selected MD user / selector and skip duplicate accepted `cycleStamp` values.
5. Fetch all configured BLM acquisitions in the same callback.
6. Filter and sort the BLM channels using `blm_positions.json`.
7. Save the raw BLM and BCT data.
8. Advance the repetition counter only when the save succeeded.

The runtime flow for live plotting is:

1. Subscribe to the same anchor device with `getHeader=True`.
2. Use the callback `anchor_value` itself as the BCT payload for that exact subscribed cycle.
3. Accept only callbacks from the selected MD user / selector and skip duplicate accepted `cycleStamp` values.
4. Fetch all BLM acquisitions inside the callback.
5. Build one integrated loss map from the configured cycle window.
6. Keep the last three loss maps in memory.
7. Update four figure windows:
   - latest total-loss trace
   - most recent loss map
   - second most recent loss map
   - third most recent loss map

The runtime flow for post-processing is:

1. Read a saved repetition parquet file.
2. Rebuild the flat saved BLM row table.
3. Integrate losses over the configured cycle window.
4. Save the figure as a 300 dpi PDF.

## Files

### `loss_map_save.py`

This is the acquisition and saving script.

Important module-level settings:

- `_SPS_USER`
  The JAPC selector / MD user.
- `_ANCHOR`
  The subscription device that defines the cycle callback.
- `_BCT`
  The BCT device used as the anchor subscription.
- `_LOSS_MAP_WINDOW_MS`
  The only loss-map window used for saving and later post-processing.
- `_BEAM_CHECK_BEFORE_MS`
  Beam must still be present before this time, otherwise the repetition does not advance.
- `_SAVE_PARQUET`
  Enables parquet writing.
- `_SAVE_JSON`
  Enables JSON writing.

Functions:

- `print_log(*args, **kwargs)`
  Prints to terminal and appends the same text to `run.log`.

- `parse_args()`
  Reads `--study-name` and `--repetitions`.

- `study_dir(study_name)`
  Returns `../sps-measurements/lossmaps/<study_name>`.

- `repetition_path(study_name, repetition)`
  Returns the parquet path for one repetition.

- `repetition_json_path(study_name, repetition)`
  Returns the JSON path for one repetition.

- `load_acquisition(japc, prefix)`
  Reads three fields from one BLM acquisition:
  `beamLossMeasurements_gray`, `beamLossMeasurementTimes_ms`, and `channelNames`.

- `main()`
  Creates JAPC, loads positions from `blm_positions.json`, tracks repetition state, defines the callback, and starts subscriptions.

Inside `main()`, the nested helper `skip_existing_results()` skips repetitions that already have a saved parquet file.

Inside `main()`, the nested callback `on_cycle(...)` does the actual work:

1. Check that `blm_positions.json` contains keys.
2. Use the callback value as the BCT payload for the exact subscribed cycle.
3. Read `anchor_header["cycleStamp"]`.
4. Skip selectors that do not match `_SPS_USER`.
5. Skip duplicate accepted `cycleStamp` values.
6. Check beam presence with `beam_injected(...)`.
7. Fetch the fixed BA/LSS BLM acquisitions.
8. Filter channels to those with known SPS positions.
9. Build `metadata`.
10. Save parquet and/or JSON.
11. Advance the repetition counter only on success.

### `loss_map_live_plot.py`

This is the live monitor.

Important module-level settings:

- `_LOSS_MAP_WINDOW_MS`
  The only loss-map window used for the live plot.
- `_SHOW_BLM_LABELS`
  Defaults to `False` so the figures stay readable.

Functions:

- `print_log(*args, **kwargs)`
  Same logging helper as in the save script.

- `parse_args()`
  Currently only builds the parser. No runtime options are required.

- `load_acquisition(japc, prefix)`
  Same BLM field fetch as in the save script.

- `main()`
  Loads the BLM position map, creates a JAPC instance, keeps a `deque(maxlen=3)` for the last three acquisitions, opens four matplotlib figures, and subscribes to the anchor.

Inside `main()`, the callback `on_cycle(...)`:

1. Uses the callback value as the BCT payload.
2. Rejects selectors that do not match `_SPS_USER`.
3. Rejects duplicate accepted `cycleStamp` values.
4. Rejects the cycle if there is no beam near the loss-map window.
5. Fetches the fixed BA/LSS BLM acquisitions.
6. Builds one integrated loss map from `_LOSS_MAP_WINDOW_MS`.
7. Pushes the latest result into a queue.

The main loop:

1. pulls the latest queued update
2. updates the total-loss figure
3. updates three separate loss-map figures

This keeps all matplotlib activity on the main thread.

### `loss_map_postprocess.py`

This script runs offline on saved parquet files.

Important module-level settings:

- `_LOSS_MAP_WINDOW_MS`
  The window used when rebuilding the loss map from saved raw data.
- `_SHOW_BLM_LABELS`
  Set to `False` if label clutter is too heavy.

Functions:

- `parse_args()`
  Reads `--study-name`.

- `main()`
  Finds all `id*.parquet` files, creates one PDF per repetition, and saves it next to the parquet file.

### `loss_map_common.py`

This file contains the shared logic.

Constants:

- `POSITIONS_FILE`
  Path to `blm_positions.json`.
- `STORAGE_ROOT`
  Base save folder: `../sps-measurements/lossmaps`.
- `COLLIMATOR_TAGS`
  Any BLM whose name contains `TCSM` or `TIDP` is plotted in black.
- `PLOT_FLOOR`
  Lower floor for logarithmic plots and normalized inefficiency.

Functions:

- `load_positions()`
  Loads `blm_positions.json` as `{channel_name: s_position}`.

- `load_blm_keys()`
  Returns only the keys from `blm_positions.json`.

- `load_blm_sources()`
  Returns the fixed BLM acquisition sources:
  BA1..6 and LSS1,2,4,5,6.

- `to_array(raw)`
  Converts JAPC values to `numpy` arrays and unwraps dictionary values if needed.

- `parquet_scalar(value)`
  Ensures parquet metadata/header values stay parquet-compatible.
  Primitive values are kept as-is.
  Complex values such as dictionaries, lists, tuples, and arrays are serialized to JSON strings.

- `cycle_timestamp_utc(cycle_stamp)`
  Converts the cycle stamp into a UTC timestamp string.

- `header_selector(header)`
  Extracts the selector / MD user from a callback header if present.

- `header_cycle_name(header)`
  Extracts the cycle name from a callback header if present.

- `acquisition_params(prefix)`
  Builds the three JAPC field names for one BLM acquisition prefix.

- `extract_waveforms(prefix, losses_raw, times_raw, channels_raw)`
  Normalizes one acquisition into:
  - a per-channel row list
  - one common time array
  - a 2D waveform array shaped like channels x times

  If the time count or channel count does not match the waveform matrix exactly, it trims to the most limiting size and emits a warning instead of aborting the measurement.

- `enrich_channels(channel_df, positions)`
  Adds `s_m` and `is_collimator`, removes unknown channels, and sorts by SPS position.

- `build_blm_frame(acquisitions, positions)`
  Builds the flat raw BLM row table used by saving, live plotting, and post-processing.

- `window_mask(times_ms, window_ms)`
  Returns the boolean mask for samples inside the configured cycle window.

- `build_snapshot(blm_frame, window_ms, cycle_stamp, floor=PLOT_FLOOR)`
  Builds one loss map from raw data:
  1. keep only rows in the window
  2. integrate each BLM loss over that window
  3. normalize each BLM loss by the integrated total loss
  4. attach cycle and plotting metadata

  This is the key function that turns the flat raw BLM rows into the plotted loss map.

- `total_losses_trace(blm_frame, window_ms=None)`
  Builds the latest total-loss trace for the live figure by summing losses at each sample time.

- `build_repetition_dataframe(blm_frame, bct_data, metadata, header)`
  Creates the flat parquet table.

  Output rows:
  - `record_type = "blm"` for BLM samples
  - `record_type = "bct"` for BCT samples

  Output columns:
  - data columns such as `channel_name`, `sample_time_ms`, `measurement_value`
  - `metadata_*` columns
  - `header_*` columns

  This keeps parquet storage simple and compatible.

- `build_repetition_payload(blm_frame, bct_data, metadata, header)`
  Creates the human-readable JSON payload with:
  - `metadata`
  - `header`
  - `data.blm`
  - `data.bct`

- `save_payload_json(path, payload)`
  Writes the JSON payload with the project’s JSON serializer.

- `load_saved_repetition(path)`
  Reads one saved parquet and reconstructs:
  - `metadata`
  - flat `blm` row table

  It also rebuilds the stored header into `metadata["header"]`.
  If post-processing looks wrong, inspect this function first.

- `plot_snapshot(ax, snapshot, show_labels=False, floor=PLOT_FLOOR)`
  Draws one loss map:
  - red for normal BLMs
  - black for `TCSM` and `TIDP`
  - optional BLM labels
  - logarithmic y-axis

- `plot_total_losses(ax, history, floor=PLOT_FLOOR)`
  Draws the latest total-loss trace figure.

- `save_loss_map_pdf(parquet_path, output_path, loss_map_window_ms, show_labels=False)`
  Loads a repetition parquet, rebuilds one integrated loss map, and saves the PDF at 300 dpi.

## Notes About Parquet

Parquet works best with flat columns and primitive values.

That is why:

- raw per-sample BLM and BCT data are stored as rows
- metadata is stored as `metadata_*` columns
- header is stored as `header_*` columns
- any complex metadata/header values are converted to JSON strings before writing

This is deliberate. It avoids awkward nested parquet schemas while remaining easy to inspect in pandas.

## Notes About JSON

The JSON mirror exists for debugging and quick manual inspection.

It is not the compact format.

Use JSON when:

- you want to see exactly what was saved tomorrow on the CCC
- you need to inspect header fields quickly
- you want a structure that looks close to the BBQ save logic

Use parquet when:

- you want smaller files
- you want faster pandas-based post-processing

## What To Check First If Something Breaks On CCC

1. Confirm the fixed BA/LSS acquisition list is correct for the machine setup.
2. Confirm `beamLossMeasurements_gray`, `beamLossMeasurementTimes_ms`, and `channelNames` are present for those sources.
3. Confirm the callback header contains the expected selector and `cycleStamp`.
4. Confirm the selected MD user really matches `_SPS_USER`.
5. Confirm the BCT device is correct and that `beam_injected(...)` is not rejecting valid cycles.
6. If plots are empty, inspect whether the configured `_LOSS_MAP_WINDOW_MS` actually overlaps the loss event.
7. If labels are unreadable or slow, keep `_SHOW_BLM_LABELS = False`.

## Current Design Choices

- One loss-map window only.
  This matches your use case where the beam is blown up only once.
- BCT is always saved together with BLM data.
- The repetition only advances after a valid save.
- No beam means the code hangs on the same repetition.
- Live plotting and post-processing use the same integrated-window snapshot logic, so they should agree if they read the same data.
