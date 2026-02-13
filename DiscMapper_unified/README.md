# DiscMapper UNIFIED v0.3 — Fixed Package

This package contains **DiscMapper UNIFIED v0.3** plus hardened engines for:
- **Movies** (`App/discmapper_v02.py`)
- **TV** (`App/discmapper_tv_v02.py`)

## Folder layout (install root)

```
DiscMapper_UNIFIED_v03/
  App/
    discmapper_unified_v03.py
    discmapper_v02.py
    discmapper_tv_v02.py
    config.json
    config_tv.json
  Inputs/
    CLZ_export.csv
    tv_manifest.csv
  Data/
    Indexes/
    Queues/
  Logs/
  Staging/
    Movies/1_Raw  Movies/2_Review  Movies/3_Ready
    TV/1_Raw      TV/2_Review      TV/3_Ready
    Unable_to_Read/
```

## What changed / fixed

### Movies engine fixes
- Restored the missing `finish_success()` (was accidentally pasted after a `return`).
- Removed unreachable junk code in `migrate_config_paths()`.
- Added robust file/folder moving (`safe_move`) to survive Windows Defender locks.
- On success, archives the job folder to `Staging/Movies/1_Raw/_done` (keeps logs/receipts) and puts the final MKV in `Staging/Movies/3_Ready`.

### TV engine improvements (best-of v0.2 + v0.2.1)
- Manifest-driven `minlength` so **short episodes (anime)** don’t get dropped.
- Robust matching (DP) using runtime windows + typical runtime.
- Uses `safe_move` for Windows lock issues.
- Writes `.discmapper.json` sidecars next to renamed episodes (optional).

## Run (PowerShell)

From the install root:

```
python .\App\discmapper_unified_v03.py
```

Or CLI:

```
python .\App\discmapper_unified_v03.py health
python .\App\discmapper_unified_v03.py refresh-all
python .\App\discmapper_unified_v03.py build-queue
python .\App\discmapper_unified_v03.py run
```

## Inputs

- Put your CLZ export at: `Inputs\CLZ_export.csv`
- Put your TV manifest at: `Inputs\tv_manifest.csv`

## Notes
- Make sure `ffprobe` is available in PATH (install FFmpeg), or set `ffprobe_path` in `App\config_tv.json`.
- Make sure MakeMKV is installed, or set the correct path in both config files.


## Automation-ready CLI commands

- `health`, `refresh-all`, `build-queue`, `run`
- `movies`, `tv`, `dry-run`
- `--yes` auto-confirms queue bootstrap prompts
- `--verbose` also prints runtime log lines to console

Examples:

```
python .\App\discmapper_unified_v03.py movies
python .\App\discmapper_unified_v03.py tv
python .\App\discmapper_unified_v03.py dry-run --yes
python .\App\discmapper_unified_v03.py run --yes --verbose
```

Each run writes a log file to `Logs/run_YYYYMMDD_HHMMSS.log`.


## Timing and policy controls

Both `App/config.json` (movies) and `App/config_tv.json` (tv) now include:

```json
"timing": {
  "poll_interval_seconds": 3,
  "disc_settle_seconds": 5,
  "post_rip_settle_seconds": 3,
  "eject_delay_seconds": 2,
  "max_wait_minutes": 30
},
"policy": {
  "keep_raw": true,
  "keep_staging": true,
  "cleanup_on_success": false,
  "eject_on_success": true,
  "eject_on_error": false,
  "safe_commit": true
}
```

Movies ripping now runs a deterministic state machine (`WAIT_FOR_DISC` → `DISC_DETECTED` → `RIP` → `VERIFY_OUTPUTS` → `PLAN_RENAME` → `COMMIT_MOVES` → `EJECT`/`DONE`) and logs per-state enter/exit timestamps plus a run summary (wait/rip/verify/move timings, raw/eject decisions).

For validation without hardware, run:

```
python .\App\discmapper_v02.py --config .\App\config.json rip --queue .\Data\Queues\queue.json --dry-run
```
