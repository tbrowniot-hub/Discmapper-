#!/usr/bin/env python3
"""
DiscMapper Movies Engine (v0.2, hardened for UNIFIED v0.3)

What this does:
- Imports a CLZ CSV export to clz_index.json (movies only)
- GUI queue builder writes queue.json (ordered list)
- Rip queue:
  - waits for disc insert
  - MakeMKV rips ALL titles into a job folder under Staging/Movies/1_Raw
  - ffprobe analyzes titles
  - keeper selection:
      * if multiple "feature-length" cuts detected, prompts user to pick
      * otherwise auto-picks best candidate
  - moves/renames keeper to Staging/Movies/3_Ready using Plex-friendly name + {imdb-tt####}
  - moves job folder to:
      * Staging/Movies/2_Review when uncertain
      * Staging/Unable_to_Read when rip fails / no MKVs
      * Staging/Movies/1_Raw/_done on success (keeps logs + receipts)

CLI:
  python discmapper_v02.py --config App/config.json import-clz --clz Inputs/CLZ_export.csv --out Data/Indexes/clz_index.json
  python discmapper_v02.py --config App/config.json queue --index Data/Indexes/clz_index.json --out Data/Queues/queue.json
  python discmapper_v02.py --config App/config.json rip --queue Data/Queues/queue.json
"""
from __future__ import annotations

import argparse
import csv
from enum import Enum
import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from logging_helper import init_run_logger, log_step

IMDB_RE = re.compile(r"(tt\d{5,10})", re.IGNORECASE)
ILLEGAL_WIN_CHARS = r'<>:"/\\|?*'

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def sanitize_name(name: str) -> str:
    cleaned = "".join("_" if c in ILLEGAL_WIN_CHARS else c for c in (name or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    return cleaned or "UNKNOWN"

def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)

def read_json_any(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))

DEFAULT_CONFIG: Dict[str, Any] = {
    # Staging (auto-migrated to current install root when config lives in App/)
    "raw_root": "Staging/Movies/1_Raw",
    "done_root": "Staging/Movies/1_Raw/_done",
    "ready_root": "Staging/Movies/3_Ready",
    "workbench_review": "Staging/Movies/2_Review",
    "workbench_unable": "Staging/Unable_to_Read",

    # MakeMKV
    "makemkv": {
        "makemkvcon_path": r"C:\Program Files (x86)\MakeMKV\makemkvcon64.exe",
        "drive_index": "auto",             # "auto" or "0", "1", ...
        "minlength_seconds": 2700          # 45 min floor for ripping "all" titles
    },

    # Eject
    "eject": {
        "enabled": True,
        "drive_letter": ""                 # empty = auto-detect first CDROM
    },

    # Keeper selection
    "min_main_minutes": 45,
    "finish": {
        "dedupe_duration_tolerance_seconds": 2,
        "multi_cut_threshold_seconds": 180
    },

    # File handling
    "move_mode": "move",                  # "move" or "copy"
    "archive_raw_on_success": True,
    "append_pkg_index_to_name": True,
    "write_sidecar_json": True,

    # Deterministic orchestration controls
    "timing": {
        "poll_interval_seconds": 3,
        "disc_settle_seconds": 5,
        "post_rip_settle_seconds": 3,
        "eject_delay_seconds": 2,
        "max_wait_minutes": 30,
    },
    "policy": {
        "keep_raw": True,
        "keep_staging": True,
        "cleanup_on_success": False,
        "eject_on_success": True,
        "eject_on_error": False,
        "safe_commit": True,
    },
}


class RipState(str, Enum):
    WAIT_FOR_DISC = "WAIT_FOR_DISC"
    DISC_DETECTED = "DISC_DETECTED"
    RIP = "RIP"
    VERIFY_OUTPUTS = "VERIFY_OUTPUTS"
    PLAN_RENAME = "PLAN_RENAME"
    COMMIT_MOVES = "COMMIT_MOVES"
    EJECT = "EJECT"
    DONE = "DONE"
    ERROR = "ERROR"

def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    """Load config.json and auto-migrate paths to the current install root."""
    if config_path:
        p = Path(config_path)
        if not p.exists():
            # create default at requested path
            ensure_dir(p.parent)
            atomic_write_json(p, DEFAULT_CONFIG)
        cfg = read_json_any(p)
        return migrate_config_paths(cfg, config_path=p)

    # default: config.json next to this script
    p = Path(__file__).with_name("config.json")
    if not p.exists():
        atomic_write_json(p, DEFAULT_CONFIG)
    cfg = read_json_any(p)
    return migrate_config_paths(cfg, config_path=p)

def migrate_config_paths(cfg: Dict[str, Any], config_path: Path) -> Dict[str, Any]:
    """Normalize/migrate staging paths for the current install root (parent of App/)."""
    cfg = dict(DEFAULT_CONFIG) | dict(cfg or {})
    root = config_path.resolve().parent.parent  # App/.. = install root

    def norm_path(v: str, default_rel: str) -> str:
        if not v:
            v = default_rel
        s = str(v)
        # Replace legacy install root if user copied configs forward
        s = s.replace(r"C:\\DiscMapper_UNIFIED_v02", str(root))
        pv = Path(s)
        if not pv.is_absolute():
            pv = root / pv
        return str(pv)

    cfg["raw_root"] = norm_path(cfg.get("raw_root",""), "Staging/Movies/1_Raw")
    cfg["done_root"] = norm_path(cfg.get("done_root",""), "Staging/Movies/1_Raw/_done")
    cfg["ready_root"] = norm_path(cfg.get("ready_root",""), "Staging/Movies/3_Ready")
    cfg["workbench_review"] = norm_path(cfg.get("workbench_review",""), "Staging/Movies/2_Review")
    cfg["workbench_unable"] = norm_path(cfg.get("workbench_unable",""), "Staging/Unable_to_Read")

    for k in ("raw_root","done_root","ready_root","workbench_review","workbench_unable"):
        try:
            ensure_dir(Path(cfg[k]))
        except Exception:
            pass

    return cfg

@dataclass
class MovieRow:
    # Stable package index (1-based row number from CLZ export)
    index: int
    # Physical barcode/UPC/EAN as string (normalized; never scientific notation)
    barcode: str

    title: str
    year: Optional[int]
    imdb_id: str
    format: str

def extract_imdb_id(url: str) -> Optional[str]:
    if not url:
        return None
    m = IMDB_RE.search(url)
    return m.group(1).lower() if m else None

def normalize_barcode(raw: str) -> str:
    """Normalize CLZ barcode/UPC/EAN strings (handles scientific notation like 7.9602E+11)."""
    s = (raw or "").strip()
    if not s:
        return ""
    # already digits
    if re.fullmatch(r"\d+", s):
        return s
    # scientific notation / decimals -> integer string (best-effort)
    if re.fullmatch(r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", s):
        try:
            from decimal import Decimal
            n = Decimal(s)
            return str(int(n))
        except Exception:
            pass
    # last resort: strip non-digits
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or s



def read_clz_csv(csv_path: Path) -> List[MovieRow]:
    raw = csv_path.read_bytes()
    text = raw.decode("utf-8-sig", errors="replace")
    rdr = csv.DictReader(text.splitlines())
    movies: List[MovieRow] = []

    for row_num, row in enumerate(rdr, start=1):
        # Some CLZ exports include this, some don't. If missing => treat as not TV.
        is_tv = (row.get("Is TV Series") or row.get("Is Tv Series") or "").strip().lower()
        if is_tv in ("yes", "true", "1"):
            continue

        title = (row.get("Title") or "").strip()
        if not title:
            continue

        year_str = (row.get("Release Year") or row.get("Year") or "").strip()
        year = int(float(year_str)) if year_str and str(year_str).replace(".","",1).isdigit() else None

        imdb_url = (row.get("IMDb Url") or row.get("IMDB Url") or "").strip()
        imdb_id = extract_imdb_id(imdb_url)
        if not imdb_id:
            # Movies automation depends on IMDb id to force Plex matching
            continue

        fmt = (row.get("Format") or "").strip()

        barcode_raw = (row.get("Barcode") or row.get("UPC") or row.get("Upc") or "").strip()
        barcode = normalize_barcode(barcode_raw)

        movies.append(MovieRow(index=row_num, barcode=barcode, title=title, year=year, imdb_id=imdb_id, format=fmt))

    return movies

def cmd_import_clz(args: argparse.Namespace) -> None:
    clz = Path(args.clz)
    if not clz.exists():
        raise FileNotFoundError(f"CLZ export not found: {clz}")
    movies = read_clz_csv(clz)
    out = Path(args.out)
    ensure_dir(out.parent)
    atomic_write_json(out, {"created_at": now_ts(), "source": str(clz), "movies": [m.__dict__ for m in movies]})
    print(f"[DiscMapper Movies] Wrote index: {out} ({len(movies)} movies)")

def cmd_queue(args: argparse.Namespace) -> None:
    import tkinter as tk
    from tkinter import ttk, messagebox

    idx_path = Path(args.index)
    if not idx_path.exists():
        raise FileNotFoundError(f"Index not found: {idx_path}")
    idx = read_json_any(idx_path)
    movies = idx.get("movies", [])

    out_path = Path(args.out)

    root = tk.Tk()
    root.title("DiscMapper — Build Rip Queue (Movies)")
    root.geometry("1200x680")

    frm = ttk.Frame(root, padding=10)
    frm.pack(fill="both", expand=True)

    left = ttk.Frame(frm)
    left.pack(side="left", fill="both", expand=True, padx=(0, 10))

    right = ttk.Frame(frm)
    right.pack(side="right", fill="both", expand=True)

    ttk.Label(left, text="Search (title / imdb / barcode)").pack(anchor="w")
    search_var = tk.StringVar()
    ent = ttk.Entry(left, textvariable=search_var)
    ent.pack(fill="x", pady=(0, 6))

    cols = ("idx", "title", "year", "format", "imdb", "barcode")
    tree = ttk.Treeview(left, columns=cols, show="headings", height=22)
    for c, t, w in [
        ("idx", "IDX", 60),
        ("title", "Title", 420),
        ("year", "Year", 70),
        ("format", "Format", 90),
        ("imdb", "IMDb", 110),
        ("barcode", "Barcode", 140),
    ]:
        tree.heading(c, text=t)
        tree.column(c, width=w, anchor="w")
    tree.pack(fill="both", expand=True)

    ttk.Label(right, text="Rip queue (top → bottom)").pack(anchor="w")

    qcols = ("idx", "title", "year", "format", "imdb", "barcode")
    qtree = ttk.Treeview(right, columns=qcols, show="headings", height=22)
    # Show only IDX + Title; keep the rest hidden but stored in item values.
    qtree.heading("idx", text="IDX"); qtree.column("idx", width=60, anchor="w")
    qtree.heading("title", text="Title"); qtree.column("title", width=430, anchor="w")

    for hidden in ("year", "format", "imdb", "barcode"):
        qtree.heading(hidden, text="")
        qtree.column(hidden, width=0, stretch=False)

    qtree.pack(fill="both", expand=True)

    def normalize_movie(m: dict) -> dict:
        # tolerate older index json shapes
        imdb = (m.get("imdb_id") or m.get("imdb") or "").strip().lower()
        bc = normalize_barcode(str(m.get("barcode") or m.get("Barcode") or ""))
        idxv = m.get("index", m.get("idx"))
        try:
            idxv = int(idxv) if idxv is not None and str(idxv).strip() != "" else None
        except Exception:
            idxv = None
        return {
            "index": idxv,
            "title": (m.get("title") or "").strip(),
            "year": m.get("year"),
            "format": (m.get("format") or "").strip(),
            "imdb_id": imdb,
            "barcode": bc,
        }

    norm_movies = [normalize_movie(m) for m in movies]

    def populate(ft: str = "") -> None:
        tree.delete(*tree.get_children())
        s = (ft or "").strip().lower()
        for m in norm_movies:
            title = m.get("title", "")
            year = m.get("year", "")
            fmt = m.get("format", "")
            imdb = m.get("imdb_id", "")
            bc = m.get("barcode", "")
            idxv = m.get("index", "")
            hay = f"{idxv} {title} {year} {fmt} {imdb} {bc}".lower()
            if s and s not in hay:
                continue
            tree.insert("", "end", values=(idxv, title, year, fmt, imdb, bc))

    def on_search(*_):
        populate(search_var.get())

    search_var.trace_add("write", on_search)
    populate("")

    def selected(t: ttk.Treeview):
        sel = t.selection()
        return t.item(sel[0], "values") if sel else None

    btnrow = ttk.Frame(right)
    btnrow.pack(fill="x", pady=8)

    def add():
        v = selected(tree)
        if v:
            qtree.insert("", "end", values=v)

    def remove():
        for s in qtree.selection():
            qtree.delete(s)

    def up():
        sel = qtree.selection()
        if not sel:
            return
        item = sel[0]
        prev = qtree.prev(item)
        if prev:
            qtree.move(item, "", qtree.index(prev))

    def down():
        sel = qtree.selection()
        if not sel:
            return
        item = sel[0]
        nxt = qtree.next(item)
        if nxt:
            qtree.move(item, "", qtree.index(nxt) + 1)

    def save(show_popup: bool = True) -> bool:
        items = []
        for iid in qtree.get_children():
            idxv, title, year, fmt, imdb, bc = qtree.item(iid, "values")
            items.append({
                "index": int(idxv) if str(idxv).strip().isdigit() else None,
                "title": str(title),
                "year": int(year) if str(year).strip().isdigit() else None,
                "format": str(fmt),
                "imdb_id": str(imdb).lower(),
                "barcode": normalize_barcode(str(bc)),
            })

        if not items:
            if show_popup:
                messagebox.showwarning("DiscMapper", "Queue is empty.")
            return False

        ensure_dir(out_path.parent)
        out_path.write_text(
            json.dumps({"created_at": now_ts(), "index_file": str(idx_path), "items": items}, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        if show_popup:
            messagebox.showinfo("DiscMapper", f"Saved queue:\n{out_path}")
        return True

    def save_and_close():
        save(show_popup=True)
        root.destroy()

    def on_close():
        # bulletproof: if user closes window, auto-save if there are items (no popup)
        if qtree.get_children():
            try:
                save(show_popup=False)
            except Exception:
                pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)

    ttk.Button(btnrow, text="Add →", command=add).pack(side="left", padx=(0, 6))
    ttk.Button(btnrow, text="Remove", command=remove).pack(side="left", padx=(0, 6))
    ttk.Button(btnrow, text="Up", command=up).pack(side="left", padx=(0, 6))
    ttk.Button(btnrow, text="Down", command=down).pack(side="left", padx=(0, 6))

    ttk.Button(btnrow, text="Save Queue", command=lambda: save(show_popup=True)).pack(side="right", padx=(6, 0))
    ttk.Button(btnrow, text="Save & Close", command=save_and_close).pack(side="right")

    # convenience: double-click in left list to add
    tree.bind("<Double-1>", lambda _e: add())
    ent.focus_set()

    root.mainloop()

def find_makemkvcon(cfg_path: str) -> Optional[str]:
    if cfg_path and Path(cfg_path).exists():
        return cfg_path
    candidates = [
        r"C:\Program Files (x86)\MakeMKV\makemkvcon64.exe",
        r"C:\Program Files\MakeMKV\makemkvcon64.exe",
        r"C:\Program Files (x86)\MakeMKV\makemkvcon.exe",
        r"C:\Program Files\MakeMKV\makemkvcon.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    for exe in ("makemkvcon64.exe","makemkvcon.exe"):
        p = shutil.which(exe)
        if p:
            return p
    return None

def detect_drive_index(makemkvcon: str) -> str:
    """Return the first MakeMKV drive index (as string)."""
    try:
        out = subprocess.check_output(
            [makemkvcon, "-r", "info", "disc:9999"],
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except Exception:
        return "0"
    for line in out.splitlines():
        if line.startswith("DRV:"):
            m = re.match(r"DRV:(\d+),", line)
            return m.group(1) if m else "0"
    return "0"

def get_optical_drive_letter() -> Optional[str]:
    ps = "(Get-CimInstance Win32_CDROMDrive | Select-Object -First 1 -ExpandProperty Drive)"
    try:
        out = subprocess.check_output(
            ["powershell","-NoProfile","-Command",ps],
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        ).strip()
        if out and re.match(r"^[A-Z]:$", out.upper()):
            return out.upper()
    except Exception:
        return None
    return None

def is_media_loaded() -> bool:
    ps = "(Get-CimInstance Win32_CDROMDrive | Select-Object -First 1 -ExpandProperty MediaLoaded)"
    try:
        out = subprocess.check_output(
            ["powershell","-NoProfile","-Command",ps],
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        ).strip().lower()
        return out in ("true","1")
    except Exception:
        return False

def eject_drive(letter: str) -> None:
    if not letter:
        return
    letter = letter.rstrip("\\/").upper()
    if not letter.endswith(":"):
        letter += ":"
    ps = f'(New-Object -comObject Shell.Application).NameSpace(17).ParseName("{letter}").InvokeVerb("Eject")'
    subprocess.run(["powershell","-NoProfile","-Command",ps], check=False)

def load_timing_policy(config: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    timing_defaults = DEFAULT_CONFIG["timing"]
    policy_defaults = DEFAULT_CONFIG["policy"]
    timing = dict(timing_defaults) | dict(config.get("timing") or {})
    policy = dict(policy_defaults) | dict(config.get("policy") or {})
    return timing, policy


def verify_disc_structure(drive_letter: str) -> bool:
    if not drive_letter:
        return False
    root = Path(f"{drive_letter}:/")
    return (root / "VIDEO_TS").exists() or (root / "BDMV").exists()


def build_finish_plan(config: Dict[str, Any], job: Dict[str, Any], keeper_path: Path) -> Dict[str, Any]:
    title = job["title"]
    year = job.get("year")
    imdb_id = job["imdb_id"]

    safe_title = sanitize_name(title)
    pkg_index = job.get("pkg_index")
    if pkg_index is None:
        pkg_index = job.get("index", job.get("idx"))
    try:
        pkg_index = int(pkg_index) if pkg_index is not None and str(pkg_index).strip() != "" else None
    except Exception:
        pkg_index = None

    idx_tag = ""
    if bool(config.get("append_pkg_index_to_name", True)) and pkg_index is not None:
        idx_tag = f" [IDX{pkg_index}]"

    if year:
        folder_name = f"{safe_title} ({year}) {{imdb-{imdb_id}}}{idx_tag}"
        file_name = f"{safe_title} ({year}) {{imdb-{imdb_id}}}{idx_tag}.mkv"
    else:
        folder_name = f"{safe_title} {{imdb-{imdb_id}}}{idx_tag}"
        file_name = f"{safe_title} {{imdb-{imdb_id}}}{idx_tag}.mkv"

    dest_dir = Path(config["ready_root"]) / folder_name
    dest = dest_dir / file_name
    if dest.exists():
        stem, suf = dest.stem, dest.suffix
        n = 1
        while True:
            cand = dest_dir / f"{stem} ({n}){suf}"
            if not cand.exists():
                dest = cand
                break
            n += 1

    return {"source": str(keeper_path), "dest_dir": str(dest_dir), "dest": str(dest), "pkg_index": pkg_index}


def run_cmd(cmd: List[str], log_path: Path) -> int:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8", errors="replace") as f:
        f.write("CMD: " + " ".join(cmd) + "\n\n")
        f.flush()
        p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, text=True)
        return p.wait()

def ffprobe_json(path: Path) -> Dict[str, Any]:
    cmd = ["ffprobe","-v","error","-show_format","-show_streams","-print_format","json",str(path)]
    out = subprocess.check_output(cmd, text=True, encoding="utf-8", errors="replace")
    return json.loads(out)

def duration_seconds(probe: Dict[str, Any]) -> float:
    fmt = probe.get("format", {}) or {}
    try:
        return float(fmt.get("duration", 0.0))
    except Exception:
        return 0.0

def summarize_streams(probe: Dict[str, Any]) -> Tuple[int,int,int]:
    streams = probe.get("streams") or []
    a = sum(1 for s in streams if s.get("codec_type")=="audio")
    sub = sum(1 for s in streams if s.get("codec_type")=="subtitle")
    v = sum(1 for s in streams if s.get("codec_type")=="video")
    return v,a,sub

def prompt_real_cut_choice(folder_name: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    import tkinter as tk
    from tkinter import ttk, messagebox

    root = tk.Tk()
    root.title("DiscMapper — Help identify real cut")
    root.geometry("980x520")

    top = ttk.Frame(root, padding=10); top.pack(fill="both", expand=True)
    ttk.Label(
        top,
        text=f"Multiple feature-length cuts detected in:\n{folder_name}\n\nSelect ONE cut to keep, or send everything to Review.",
        justify="left"
    ).pack(anchor="w", pady=(0,10))

    cols=("duration","size_gb","audio","subs","file")
    tree = ttk.Treeview(top, columns=cols, show="headings", height=14)
    for c,t,w,anch in [
        ("duration","Duration (min)",110,"e"),
        ("size_gb","Size (GB)",90,"e"),
        ("audio","Audio tracks",110,"e"),
        ("subs","Subtitle tracks",130,"e"),
        ("file","Filename",480,"w")
    ]:
        tree.heading(c, text=t); tree.column(c, width=w, anchor=anch)
    tree.pack(fill="both", expand=True)

    for c in candidates:
        tree.insert("", "end", values=(c["duration_min"], c["size_gb"], c["audio_streams"], c["subtitle_streams"], c["name"]), tags=(c["path"],))

    items = tree.get_children()
    if items:
        tree.selection_set(items[0])

    choice: Dict[str, Any] = {"action":"review"}

    def selected_path() -> Optional[str]:
        sel=tree.selection()
        if not sel:
            return None
        tags=tree.item(sel[0],"tags")
        return tags[0] if tags else None

    btns=ttk.Frame(top); btns.pack(fill="x", pady=(10,0))

    def keep_selected():
        p=selected_path()
        if not p:
            messagebox.showwarning("DiscMapper","Select a cut first.")
            return
        choice["action"]="keep_one"; choice["path"]=p; root.destroy()

    def keep_all():
        choice["action"]="review_keep_all"; root.destroy()

    def review():
        choice["action"]="review"; root.destroy()

    ttk.Button(btns, text="Keep SELECTED cut (recommended)", command=keep_selected).pack(side="left")
    ttk.Button(btns, text="Keep ALL (send folder to Review)", command=keep_all).pack(side="left", padx=8)
    ttk.Button(btns, text="Send to Review (decide later)", command=review).pack(side="right")

    root.mainloop()
    return choice

def pick_keeper_or_prompt(config: Dict[str, Any], folder: Path, min_main_seconds: int) -> Dict[str, Any]:
    tol = int((config.get("finish") or {}).get("dedupe_duration_tolerance_seconds", 2))
    multi_cut = int((config.get("finish") or {}).get("multi_cut_threshold_seconds", 180))

    mkvs = sorted(folder.glob("*.mkv"))
    if not mkvs:
        return {"status":"review","reason":"no_mkv_files"}

    infos: List[Dict[str, Any]] = []
    for f in mkvs:
        try:
            probe = ffprobe_json(f)
            dur = duration_seconds(probe)
            v,a,sub = summarize_streams(probe)
        except Exception:
            dur = 0.0; a=sub=v=0
        infos.append({
            "path": str(f),
            "name": f.name,
            "size_bytes": f.stat().st_size,
            "size_gb": round(f.stat().st_size/(1024**3),3),
            "duration_sec": dur,
            "duration_min": round(dur/60.0,2) if dur else 0.0,
            "audio_streams": a,
            "subtitle_streams": sub
        })

    candidates = [x for x in infos if x["duration_sec"] >= min_main_seconds]
    if not candidates:
        return {"status":"review","reason":f"no_candidate_over_{min_main_seconds//60}m"}

    candidates.sort(key=lambda x:(x["duration_sec"], x["size_bytes"]), reverse=True)

    # Cluster by near-identical duration (dedupe tolerance)
    clusters: List[List[Dict[str, Any]]] = []
    for it in candidates:
        placed=False
        for cl in clusters:
            if abs(cl[0]["duration_sec"] - it["duration_sec"]) <= tol:
                cl.append(it); placed=True; break
        if not placed:
            clusters.append([it])

    if len(clusters) > 1:
        d0 = clusters[0][0]["duration_sec"]; d1 = clusters[1][0]["duration_sec"]
        if abs(d0 - d1) > multi_cut:
            choice = prompt_real_cut_choice(folder.name, candidates)
            if choice["action"] == "review_keep_all":
                return {"status":"review","reason":"user_chose_keep_all_to_review","candidates":candidates}
            if choice["action"] == "review":
                return {"status":"review","reason":"user_sent_to_review","candidates":candidates}
            if choice["action"] == "keep_one":
                return {"status":"success","keeper_path":choice["path"],"candidates":candidates,"reason":"user_selected_cut"}
            return {"status":"review","reason":"no_user_choice","candidates":candidates}

    # Auto pick: largest file inside best duration cluster
    keeper = max(clusters[0], key=lambda x:x["size_bytes"])
    return {"status":"success","keeper_path":keeper["path"],"candidates":candidates,"reason":"auto_selected_best_candidate"}

def safe_move(src: Path, dst: Path, retries: int = 25, delay: float = 0.4) -> None:
    """Robust move for Windows where files/dirs may be temporarily locked."""
    last = None
    for attempt in range(retries):
        try:
            ensure_dir(dst.parent)
            shutil.move(str(src), str(dst))
            return
        except Exception as e:
            last = e
            time.sleep(delay * (attempt + 1))

    # Fallback: copy then delete (best-effort)
    ensure_dir(dst.parent)
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=False)
        for attempt in range(retries):
            try:
                shutil.rmtree(src)
                break
            except Exception:
                time.sleep(delay * (attempt + 1))
    else:
        shutil.copy2(src, dst)
        for attempt in range(retries):
            try:
                src.unlink(missing_ok=True)
                break
            except Exception:
                time.sleep(delay * (attempt + 1))

    if not dst.exists():
        raise RuntimeError(f"safe_move failed: {src} -> {dst}. Last error: {last}")

def move_folder(folder: Path, target_root: Path) -> Path:
    """Move a whole job folder to a target root (robust)."""
    ensure_dir(target_root)
    dest = target_root / folder.name
    if dest.exists():
        dest = target_root / f"{folder.name}__{now_ts()}"
    safe_move(folder, dest)
    return dest

def finish_success(
    config: Dict[str, Any],
    job: Dict[str, Any],
    job_dir: Path,
    keeper_path: str,
    candidates: List[Dict[str, Any]],
    reason: str
) -> Dict[str, Any]:
    """Move/rename keeper into Ready and return receipt dict."""
    plan = build_finish_plan(config, job, Path(keeper_path))
    kp = Path(plan["source"])
    dest = Path(plan["dest"])
    ensure_dir(Path(plan["dest_dir"]))
    if str(config.get("move_mode","move")).lower() == "copy":
        ensure_dir(dest.parent)
        shutil.copy2(kp, dest)
    else:
        safe_move(kp, dest)

    if bool(config.get("write_sidecar_json", True)):
        sidecar = dest.with_name(dest.stem + ".discmapper.json")
        meta = {
            "type": "movie",
            "title": job["title"],
            "year": job.get("year"),
            "imdb_id": job["imdb_id"],
            "pkg_index": plan["pkg_index"],
            "barcode": job.get("barcode",""),
            "reason": reason,
            "candidates": candidates,
            "job_dir": str(job_dir),
            "keeper_source": str(kp),
            "keeper_dest": str(dest),
            "completed_at": now_ts(),
        }
        try:
            atomic_write_json(sidecar, meta)
        except Exception:
            pass

    return {
        "status":"success",
        "reason": reason,
        "keeper_dest": str(dest),
        "candidates": candidates,
        "completed_at": now_ts()
    }

def cmd_rip(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    timing, policy = load_timing_policy(config)

    qpath = Path(args.queue)
    if not qpath.exists():
        raise FileNotFoundError(f"Queue not found: {qpath}")
    q = read_json_any(qpath)
    items = q.get("items", [])
    if not items:
        print("[DiscMapper Movies] Queue is empty.")
        return

    dry_run = bool(getattr(args, "dry_run", False))

    mk_cfg = (config.get("makemkv") or {})
    makemkv = find_makemkvcon(str(mk_cfg.get("makemkvcon_path") or ""))
    if not makemkv and not dry_run:
        raise RuntimeError("MakeMKV CLI not found. Install MakeMKV and/or set makemkv.makemkvcon_path.")

    drive_index = str(mk_cfg.get("drive_index") or "auto").strip().lower()
    if drive_index in ("", "auto"):
        drive_index = detect_drive_index(makemkv) if makemkv else "0"

    minlength = int(mk_cfg.get("minlength_seconds", 2700))

    eject_cfg = (config.get("eject") or {})
    eject_enabled = bool(eject_cfg.get("enabled", True))
    drive_letter = str(eject_cfg.get("drive_letter") or "").strip()
    if not drive_letter:
        drive_letter = get_optical_drive_letter() or ""

    raw_root = Path(config["raw_root"]); ensure_dir(raw_root)
    done_root = Path(config.get("done_root") or (raw_root / "_done")); ensure_dir(done_root)
    review_root = Path(config["workbench_review"]); ensure_dir(review_root)
    unable_root = Path(config["workbench_unable"]); ensure_dir(unable_root)
    min_main = int(float(config.get("min_main_minutes", 45)) * 60)

    print(f"[DiscMapper Movies] MakeMKV: {makemkv or 'dry-run'}")
    print(f"[DiscMapper Movies] Drive index: {drive_index}")
    print(f"[DiscMapper Movies] Dry-run: {'ON' if dry_run else 'OFF'}")
    print(f"[DiscMapper Movies] Timing: {timing}")
    print(f"[DiscMapper Movies] Policy: {policy}")

    total = len(items)
    append_idx = bool(config.get("append_pkg_index_to_name", True))

    for qpos, it in enumerate(items, start=1):
        state_times: Dict[str, Dict[str, float]] = {}
        state_order: List[str] = []
        errored = False
        verify_ok = False
        commit_ok = False
        ejected = False
        wait_time = rip_time = verify_time = move_time = 0.0

        def enter(state: RipState, **decisions: Any) -> None:
            ts = time.time()
            state_times[state.value] = {"entered_at": ts}
            state_order.append(state.value)
            print(f"[State] {state.value} ENTER {datetime.fromtimestamp(ts).isoformat()} decisions={decisions}")

        def exit_state(state: RipState, **decisions: Any) -> None:
            ts = time.time()
            rec = state_times.get(state.value, {})
            rec["exited_at"] = ts
            state_times[state.value] = rec
            print(f"[State] {state.value} EXIT  {datetime.fromtimestamp(ts).isoformat()} decisions={decisions}")

        title = it["title"]
        year = it.get("year")
        imdb = str(it["imdb_id"]).lower()
        fmt = it.get("format", "")

        pkg_index = it.get("index", it.get("idx"))
        try:
            pkg_index = int(pkg_index) if pkg_index is not None and str(pkg_index).strip() != "" else None
        except Exception:
            pkg_index = None

        barcode = normalize_barcode(str(it.get("barcode") or it.get("Barcode") or ""))
        safe_title = sanitize_name(title)
        idx_tag = f" [IDX{pkg_index}]" if (append_idx and pkg_index is not None) else ""
        folder_name = f"{safe_title} ({year}) {{imdb-{imdb}}}{idx_tag}" if year else f"{safe_title} {{imdb-{imdb}}}{idx_tag}"
        job_dir = raw_root / folder_name
        ensure_dir(job_dir)

        atomic_write_json(job_dir / ".discmapper.job.json", {
            "type": "movie", "title": title, "year": year, "imdb_id": imdb,
            "format": fmt, "pkg_index": pkg_index, "barcode": barcode,
            "queue_pos": qpos, "queue_total": total, "created_at": now_ts(),
        })

        print(f"=== [{qpos}/{total}] NEXT DISC ===")
        print(f"Movie: {title} ({year or '????'}) [{imdb}] IDX: {pkg_index if pkg_index is not None else 'n/a'}")

        try:
            enter(RipState.WAIT_FOR_DISC, drive_letter=drive_letter, queue_pos=qpos)
            wait_started = time.monotonic()
            max_wait_s = int(float(timing.get("max_wait_minutes", 30)) * 60)
            poll_s = max(1, int(timing.get("poll_interval_seconds", 3)))
            while True:
                if dry_run or is_media_loaded():
                    break
                if (time.monotonic() - wait_started) > max_wait_s:
                    raise TimeoutError(f"Timed out waiting for disc after {max_wait_s}s")
                time.sleep(poll_s)
            wait_time = time.monotonic() - wait_started
            exit_state(RipState.WAIT_FOR_DISC, waited_seconds=round(wait_time, 2))

            enter(RipState.DISC_DETECTED, drive_letter=drive_letter)
            if not dry_run:
                time.sleep(max(0, int(timing.get("disc_settle_seconds", 5))))
                if drive_letter and not verify_disc_structure(drive_letter):
                    raise RuntimeError(f"Disc structure check failed on {drive_letter}: expected VIDEO_TS or BDMV")
            exit_state(RipState.DISC_DETECTED, structure_ok=True)

            enter(RipState.RIP, drive_index=drive_index)
            rip_started = time.monotonic()
            if dry_run:
                fake = job_dir / "title_t00.mkv"
                fake.write_bytes(b"discmapper-dry-run")
                rc = 0
            else:
                log = job_dir / f"makemkv_{now_ts()}.log"
                cmd = [makemkv, "-r", f"--minlength={minlength}", "mkv", f"disc:{drive_index}", "all", str(job_dir)]
                print(f"[DiscMapper Movies] Ripping... log: {log.name}")
                rc = run_cmd(cmd, log)
            rip_time = time.monotonic() - rip_started
            if rc != 0:
                raise RuntimeError(f"makemkv_error_rc_{rc}")
            if not dry_run:
                time.sleep(max(0, int(timing.get("post_rip_settle_seconds", 3))))
            exit_state(RipState.RIP, rip_seconds=round(rip_time, 2), rc=rc)

            enter(RipState.VERIFY_OUTPUTS)
            verify_started = time.monotonic()
            mkvs = sorted([p for p in job_dir.rglob("*.mkv") if p.is_file()])
            file_count = len(mkvs)
            if file_count == 0:
                raise RuntimeError("no_mkvs_produced")
            if not all(p.stat().st_size > 0 for p in mkvs):
                raise RuntimeError("mkv_zero_byte_file_detected")
            verify_ok = True
            verify_time = time.monotonic() - verify_started
            exit_state(RipState.VERIFY_OUTPUTS, file_count=file_count, verify_seconds=round(verify_time, 2))

            enter(RipState.PLAN_RENAME)
            if dry_run:
                keeper_res = {"status": "success", "keeper_path": str(mkvs[0]), "candidates": [], "reason": "dry_run"}
            else:
                keeper_res = pick_keeper_or_prompt(config, job_dir, min_main)
            if keeper_res.get("status") != "success":
                raise RuntimeError(f"review_required:{keeper_res.get('reason')}")
            plan = build_finish_plan(config, {"title": title, "year": year, "imdb_id": imdb, "pkg_index": pkg_index, "barcode": barcode}, Path(keeper_res["keeper_path"]))
            exit_state(RipState.PLAN_RENAME, source=plan["source"], dest=plan["dest"], title_count=file_count)

            enter(RipState.COMMIT_MOVES)
            move_started = time.monotonic()
            src = Path(plan["source"])
            dest = Path(plan["dest"])
            ensure_dir(Path(plan["dest_dir"]))
            if str(config.get("move_mode", "move")).lower() == "copy":
                shutil.copy2(src, dest)
            else:
                safe_move(src, dest)
            if policy.get("safe_commit", True):
                if not dest.exists() or dest.stat().st_size <= 0:
                    raise RuntimeError("commit_verification_failed")
            commit_ok = True
            move_time = time.monotonic() - move_started
            receipt = {
                "status": "success",
                "reason": keeper_res.get("reason", ""),
                "keeper_dest": str(dest),
                "candidates": keeper_res.get("candidates", []),
                "plan": plan,
                "completed_at": now_ts(),
            }
            atomic_write_json(job_dir / ".discmapper.receipt.json", receipt)
            exit_state(RipState.COMMIT_MOVES, move_seconds=round(move_time, 2), committed=True)

            if bool(policy.get("cleanup_on_success", False)) and not bool(policy.get("keep_raw", True)):
                src_dir = job_dir
                if src_dir.exists():
                    shutil.rmtree(src_dir, ignore_errors=True)
            elif bool(config.get("archive_raw_on_success", True)) and not bool(policy.get("keep_raw", True)):
                move_folder(job_dir, done_root)

            should_eject = (eject_enabled and bool(policy.get("eject_on_success", True)) and verify_ok and commit_ok)
            if should_eject and drive_letter:
                enter(RipState.EJECT, drive_letter=drive_letter)
                time.sleep(max(0, int(timing.get("eject_delay_seconds", 2))))
                eject_drive(drive_letter)
                ejected = True
                exit_state(RipState.EJECT, ejected=True)

            enter(RipState.DONE)
            exit_state(RipState.DONE, title=title)

        except Exception as e:
            errored = True
            enter(RipState.ERROR, error=repr(e))
            atomic_write_json(job_dir / ".discmapper.receipt.json", {"status": "unable", "reason": str(e), "completed_at": now_ts()})
            if job_dir.exists() and not dry_run:
                move_folder(job_dir, unable_root)
            if eject_enabled and bool(policy.get("eject_on_error", False)) and drive_letter:
                enter(RipState.EJECT, drive_letter=drive_letter)
                time.sleep(max(0, int(timing.get("eject_delay_seconds", 2))))
                eject_drive(drive_letter)
                ejected = True
                exit_state(RipState.EJECT, ejected=True, on_error=True)
            exit_state(RipState.ERROR)

        print("[DiscMapper Movies] Run summary:")
        print(f"  disc wait time: {wait_time:.2f}s")
        print(f"  rip time: {rip_time:.2f}s")
        print(f"  verify time: {verify_time:.2f}s")
        print(f"  rename/move time: {move_time:.2f}s")
        print(f"  raw kept: {bool(policy.get('keep_raw', True))}")
        print(f"  ejected: {ejected}")
        print(f"  errored: {errored}")
        print(f"  states: {' -> '.join(state_order)}")

    print("[DiscMapper Movies] Queue completed.")


def main() -> None:
    ap = argparse.ArgumentParser(prog="discmapper_v02")
    ap.add_argument("--config", default=None)
    sp = ap.add_subparsers(dest="cmd", required=True)

    p1 = sp.add_parser("import-clz", help="Import CLZ export CSV to an index JSON")
    p1.add_argument("--clz", required=True)
    p1.add_argument("--out", default="clz_index.json")
    p1.set_defaults(func=cmd_import_clz)

    p2 = sp.add_parser("queue", help="GUI queue builder (movies)")
    p2.add_argument("--index", default="clz_index.json")
    p2.add_argument("--out", default="queue.json")
    p2.set_defaults(func=cmd_queue)

    p3 = sp.add_parser("rip", help="Run ripping session from queue")
    p3.add_argument("--queue", default="queue.json")
    p3.add_argument("--dry-run", action="store_true", help="Run state machine without requiring hardware")
    p3.set_defaults(func=cmd_rip)

    args = ap.parse_args()
    config_used = args.config or str(Path(__file__).with_name("config.json"))
    logger = init_run_logger("discmapper_v02", mode="movies", config_used=config_used)
    log_step(logger, f"command:{args.cmd}", starting=True)
    args.func(args)
    log_step(logger, f"command:{args.cmd}", starting=False)

if __name__ == "__main__":
    main()
