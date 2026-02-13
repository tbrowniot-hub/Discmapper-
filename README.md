# DiscMapper (Windows Python Tool)

DiscMapper automates Blu-ray/DVD ripping workflows for **movies** and **TV** using MakeMKV.

## One-command launcher (recommended)

Use the repo-root launcher:

```powershell
.\run.ps1 health
```

Available launcher commands:

- `health`
- `refresh-all`
- `build-queue`
- `run`
- `movies`
- `tv`
- `dry-run` (safe movies pipeline check)

Examples:

```powershell
.\run.ps1 build-queue
.\run.ps1 run
.\run.ps1 movies
.\run.ps1 dry-run --yes
```

## Where am I?

If your **current folder already contains** `App/`, `Inputs/`, `Data/`, `Logs/`, `Staging/`, then you are already inside `DiscMapper_unified`.

### A) You are at repo root (folder contains `DiscMapper_unified/`)

Use:

```powershell
.\run.ps1 health
.\run.ps1 refresh-all
.\run.ps1 build-queue
.\run.ps1 run
```

### B) You are already inside `DiscMapper_unified/`

Use either:

```powershell
..\run.ps1 health
```

or direct Python commands:

```powershell
python .\App\discmapper_unified_v03.py health
python .\App\discmapper_unified_v03.py refresh-all
python .\App\discmapper_unified_v03.py build-queue
python .\App\discmapper_unified_v03.py run
python .\App\discmapper_unified_v03.py movies
python .\App\discmapper_unified_v03.py tv
python .\App\discmapper_unified_v03.py dry-run --yes
```

## Fresh clone setup

1. Install **Python 3.10+** (ensure `python` is in PATH).
2. Install **MakeMKV** and verify `makemkvcon64.exe` exists.
3. (Recommended for TV) install **FFmpeg** so `ffprobe` is in PATH.
4. Keep this layout:

```text
DiscMapper_unified/
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
```

5. Run health check:

```powershell
.\run.ps1 health
```

## CLI behavior notes

- `build-queue` now builds movie index+queue end-to-end (and TV queue if TV manifest exists).
- `run` will prompt to build a missing movie queue.
- `dry-run` runs movies in safe dry-run mode (`discmapper_v02.py rip --dry-run`) and does not require a disc.
- TV remains optional; missing TV manifest/queue skips TV steps.

## Smoke test script

Windows-friendly smoke script:

```powershell
.\scripts\smoke_test.ps1
.\scripts\smoke_test.ps1 -DryRun
```

This runs:

- `python -c "import sys; print(sys.version)"`
- `python App/discmapper_unified_v03.py health`
- optional `dry-run` when `-DryRun` is supplied.
