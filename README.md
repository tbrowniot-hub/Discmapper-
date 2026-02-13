# DiscMapper (Windows Python Tool)

DiscMapper helps automate Blu-ray/DVD ripping workflows for **movies** and **TV** using MakeMKV.

## Quick Start

### 1) Install dependencies
1. Install **Python 3.10+** (Windows) and ensure `python` is in PATH.
2. Install **MakeMKV** and verify `makemkvcon64.exe` exists (default path is usually under `C:\Program Files (x86)\MakeMKV\`).
3. (Recommended for TV processing) Install **FFmpeg** so `ffprobe` is available in PATH.

### 2) Prepare project files
From the repo root, keep this structure:

```text
DiscMapper_unified/
  App/
    discmapper_unified_v03.py   # unified launcher/dashboard
    discmapper_v02.py           # movies engine
    discmapper_tv_v02.py        # TV engine
    config.json                 # movies config
    config_tv.json              # TV config
  Inputs/
    CLZ_export.csv              # movies input catalog
    tv_manifest.csv             # TV manifest input
  Data/
    Indexes/                    # generated index JSON files
    Queues/                     # generated queue JSON files
  Logs/                         # per-run logs (auto-created)
  Staging/
    Movies/1_Raw, 2_Review, 3_Ready
    TV/1_Raw, 2_Review, 3_Ready
    Unable_to_Read/
```

### 3) Configure inputs
- Place CLZ movie export at: `DiscMapper_unified/Inputs/CLZ_export.csv`
- Place TV manifest at: `DiscMapper_unified/Inputs/tv_manifest.csv`

### 4) Run DiscMapper
From the repo root:

```powershell
python .\DiscMapper_unified\App\discmapper_unified_v03.py
```

Useful CLI examples:

```powershell
# health + prep commands
python .\DiscMapper_unified\App\discmapper_unified_v03.py health
python .\DiscMapper_unified\App\discmapper_unified_v03.py refresh-all
python .\DiscMapper_unified\App\discmapper_unified_v03.py build-queue
python .\DiscMapper_unified\App\discmapper_unified_v03.py run

# non-interactive mode selection
python .\DiscMapper_unified\App\discmapper_unified_v03.py --movies
python .\DiscMapper_unified\App\discmapper_unified_v03.py --tv
python .\DiscMapper_unified\App\discmapper_unified_v03.py --movies --tv
python .\DiscMapper_unified\App\discmapper_unified_v03.py --movies --verbose
```

## Notes
- Update `DiscMapper_unified/App/config.json` and `config_tv.json` if your MakeMKV/FFprobe paths differ.
- Logs are written to `DiscMapper_unified/Logs/run_YYYYMMDD_HHMMSS.log`.
