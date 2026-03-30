# SPS CCC scripts

This repo now contains two scripts:

- `run_dpp_scan.py`: save script. This is the important one.
- `live_tune_monitor.py`: optional live tune monitor that can run in parallel.
- `loss_map_save.py`: save raw BLM and BCT loss-map data to parquet.
- `loss_map_live_plot.py`: live BLM loss-map plotting for the last three acquisitions.
- `loss_map_postprocess.py`: build 300 dpi PDF loss-map figures from saved parquet files.
- `loss_map_common.py`: shared loss-map parsing, normalization, and plotting helpers.
- `loss_map_monitor.py`: compatibility stub that points to the split scripts above.
- `inspect_ccc_devices.py`: SWAN/CCC inspection helper for checking device contents.
- `bct.py`: BCT helpers for beam-presence checks.
- `bbq.py`: BBQ parsing plus a first-pass tune estimator.
- `json_utils.py`: JSON serialization helpers for `numpy` and timestamps.
- `developer.md`: project notes, assumptions, and remaining CCC questions.

## Workflow

`run_dpp_scan.py` works like this:

- subscribe to `SPS.BQ.CONT/ContinuousAcquisition`
- first callback: apply the requested `dp/p` offset
- next BBQ callbacks: fetch `BCT`, `DpOverPOffset`, `RadialSteering`, and `RevFreq`
- next BBQ callbacks: fetch `BCT`, `DpOverPOffset`, and `RevFreq`
- require beam before the measurement window
- require beam still present near the end of the cycle
- if the beam is lost or missing, do not advance; wait on the same repetition
- save one file per successful repetition only

The saved JSON includes:

- full raw BCT payload
- full raw BBQ payload
- full raw `DpOverPOffset` and `RevFreq` payloads
- BCT header
- target `dp/p`
- measured `dp/p`

Optional extras:

- save the starting `QH` and `QV` references if you set `_QH_REF` and `_QV_REF`
- save radial steering too if you set `_SAVE_RADIAL_STEERING = True`

## Important CCC note

`run_dpp_scan.py` defaults to `_APPLY_TRIMS = False`.

That is deliberate: the acquisition and save logic are ready, but the exact writeback format for `SpsLowLevelRF/DpOverPOffset` still needs to be validated on the CCC before enabling trims. The trimming hook is isolated in `build_dpp_function(...)` and `apply_offset(...)`.

## What To Check On SWAN / CCC

Use `inspect_ccc_devices.py` to inspect candidate devices and write their structure to `inspection/`.

What still needs to be confirmed:

- the exact writeback structure of `SpsLowLevelRF/DpOverPOffset`
- whether `SPS.BQ.CONT/ContinuousAcquisition` is the right BBQ source
- whether there is a better kick-based BBQ acquisition
- the correct device names for starting `QH` and `QV`
- the final timing values for:
  - start of the `dp/p` action
  - end of the `dp/p` action
  - beam survival check
  - horizontal kick window
  - vertical kick window

Basic usage:

```bash
python3 inspect_ccc_devices.py
```

Then inspect the JSON files in `inspection/`.

## Save layout

The save structure is close to Frederik's:

```text
results/<shortname>_<side>/<offset_label>/id<repetition>.json
```

Example:

```text
results/DPP_BBQ_SCAN_NEG/minus_1.000000e-03/id2.json
```

That gives:

- one folder for the scan side
- one subfolder per `dp/p` target
- three files `id1.json`, `id2.json`, `id3.json`

## Scan order

The script is one-sided by design:

- set `_SCAN_SIDE = "NEG"` and fill `_OFFSETS_NEG`
- run that side completely
- then change to `_SCAN_SIDE = "POS"` and run the other side

That matches your operational constraint that one side should be completed before crossing to the other.

## Tune extraction

`live_tune_monitor.py` is separate on purpose. It can run in parallel while the save script runs, and it assumes the kick is done manually.

Current live tune extraction is intentionally simple and inspectable:

- take an explicit time window for the horizontal kick
- take an explicit time window for the vertical kick
- apply a Hann window
- compute the FFT
- optionally bias the peak choice with an expected tune

By default `_EXPECTED_TUNE_H` and `_EXPECTED_TUNE_V` are `None`, so the live script does not assume fixed machine tunes.

This is a reasonable first pass for CCC operation. If chromaticity splitting makes the dominant peak unreliable, the next step should be to compare:

- wider candidate tracking around an expected-tune band
- a narrowband pre-filter before FFT
- `nafflib` on the kicked window

## Run

From the CCC environment, after validating the device names and the `DpOverPOffset` writeback:

```bash
python3 run_dpp_scan.py
```

Optional live plotting in another terminal:

```bash
python3 live_tune_monitor.py
```

## BLM Loss Maps

The loss-map workflow is now split into three scripts:

- [loss_map_save.py](/Users/lisepauwels/phd/code/sps-ccc-scripts/loss_map_save.py)
  subscribes once per cycle, checks that beam is present, and saves one parquet file per successful repetition.
- [loss_map_live_plot.py](/Users/lisepauwels/phd/code/sps-ccc-scripts/loss_map_live_plot.py)
  plots the last three loss maps live.
- [loss_map_postprocess.py](/Users/lisepauwels/phd/code/sps-ccc-scripts/loss_map_postprocess.py)
  reads the saved parquet files and writes 300 dpi PDF figures.

It uses the BLM keys in [blm_positions.json](/Users/lisepauwels/phd/code/sps-ccc-scripts/blm_positions.json) as the authoritative monitor list for acquisition, filtering, and plotting.

The save format is:

```text
../sps-measurements/lossmaps/<study_name>/id1.parquet
../sps-measurements/lossmaps/<study_name>/id1.json
../sps-measurements/lossmaps/<study_name>/id2.parquet
...
```

Each repetition parquet contains:

- `metadata_*` columns
- `header_*` columns
- all BLM samples for the cycle
- the BLM sample times inside the cycle
- the BCT samples for the same cycle
- the cycle name
- the MD user
- the cycle timestamp
- the SPS selector / MD user
- the configured loss-map window
- the BCT samples for the same cycle

The JSON mirror uses a more inspectable structure:

- `metadata`
- `header`
- `data.blm`
- `data.bct`

The post-processing output is written next to each parquet file:

```text
../sps-measurements/lossmaps/<study_name>/id1_lossmap.pdf
```

Each PDF contains:

- one loss map from the configured cycle window
- logarithmic y-scale
- y = loss in that BLM divided by total loss
- x = position along the ring
- `TCSM` and `TIDP` monitors in black
- all other monitors in red

For both live plotting and post-processing there is a `_SHOW_BLM_LABELS` switch in the script, so the BLM names can be disabled by simply changing that variable.
