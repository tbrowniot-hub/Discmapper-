#!/usr/bin/env python3
"""
DiscMapper UNIFIED v0.3 (Movies + TV)

This is the dashboard/orchestrator:
- Health check (folder layout + required inputs/configs)
- Refresh indexes (Movies CLZ -> clz_index.json, TV manifest -> tv_index.json)
- Build queues (Movies GUI, then TV GUI)
- Run queues (Movies rip, then TV rip). Movies failures do not block TV.

Expected folder layout (install root):
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
      clz_index.json
      tv_index.json
    Queues/
      queue.json
      tv_queue.json
  Staging/
    Movies/1_Raw, Movies/2_Review, Movies/3_Ready
    TV/1_Raw, TV/2_Review, TV/3_Ready
    Unable_to_Read/

Run:
  python App\\discmapper_unified_v03.py          # GUI
  python App\\discmapper_unified_v03.py health
  python App\\discmapper_unified_v03.py refresh-all
  python App\\discmapper_unified_v03.py build-queue
  python App\\discmapper_unified_v03.py run
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Any


def app_dir() -> Path:
    return Path(__file__).resolve().parent


def root_dir() -> Path:
    return app_dir().parent


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8-sig"))


def write_json(p: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(p.parent)
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def migrate_config_paths() -> None:
    """
    Bulletproof rule: NEVER write into older installs (e.g. DiscMapper_UNIFIED_v02).
    We auto-migrate config.json + config_tv.json to the current install root.
    """
    r = root_dir()
    app = app_dir()
    cfg_files = [app / "config.json", app / "config_tv.json"]

    for cfg_path in cfg_files:
        if not cfg_path.exists():
            continue
        bak = cfg_path.with_suffix(cfg_path.suffix + f".bak_{int(time.time())}")
        try:
            shutil.copy2(cfg_path, bak)
        except Exception:
            pass

        try:
            cfg = read_json(cfg_path)
        except Exception:
            continue

        def fix(v: Any) -> Any:
            if not isinstance(v, str):
                return v
            s = v
            s = s.replace("C:\\\\DiscMapper_UNIFIED_v02", str(r))
            s = s.replace("C:\\DiscMapper_UNIFIED_v02", str(r))
            # Resolve relative paths under root
            try:
                pv = Path(s)
                if not pv.is_absolute():
                    pv = r / pv
                s = str(pv)
            except Exception:
                pass
            return s

        changed = False
        for k, v in list(cfg.items()):
            nv = fix(v)
            if nv != v:
                cfg[k] = nv
                changed = True

        if changed:
            try:
                write_json(cfg_path, cfg)
            except Exception:
                pass

    # Ensure folder layout exists
    folders = [
        r / "Inputs",
        r / "Data" / "Indexes",
        r / "Data" / "Queues",
        r / "Logs",
        r / "Staging" / "Movies" / "1_Raw",
        r / "Staging" / "Movies" / "2_Review",
        r / "Staging" / "Movies" / "3_Ready",
        r / "Staging" / "TV" / "1_Raw",
        r / "Staging" / "TV" / "2_Review",
        r / "Staging" / "TV" / "3_Ready",
        r / "Staging" / "Unable_to_Read",
    ]
    for f in folders:
        try:
            ensure_dir(f)
        except Exception:
            pass


def default_paths() -> Dict[str, Path]:
    r = root_dir()
    return {
        "clz_csv": r / "Inputs" / "CLZ_export.csv",
        "tv_manifest": r / "Inputs" / "tv_manifest.csv",
        "movies_index": r / "Data" / "Indexes" / "clz_index.json",
        "tv_index": r / "Data" / "Indexes" / "tv_index.json",
        "movies_queue": r / "Data" / "Queues" / "queue.json",
        "tv_queue": r / "Data" / "Queues" / "tv_queue.json",
        "movies_config": app_dir() / "config.json",
        "tv_config": app_dir() / "config_tv.json",
    }


def run_cmd(cmd: list[str]) -> int:
    # Keep cwd at App/ so relative tool paths work
    return subprocess.call(cmd, cwd=str(app_dir()))


def health_check() -> None:
    migrate_config_paths()
    p = default_paths()

    problems = []

    if not p["clz_csv"].exists():
        problems.append(f"Missing CLZ export: {p['clz_csv']}")
    if not p["tv_manifest"].exists():
        problems.append(f"Missing TV manifest: {p['tv_manifest']}")

    if not p["movies_config"].exists():
        problems.append(f"Missing movies config: {p['movies_config']}")
    if not p["tv_config"].exists():
        problems.append(f"Missing TV config: {p['tv_config']}")

    # Sanity-check MakeMKV path in both configs when possible
    try:
        mcfg = read_json(p["movies_config"])
        mk = ((mcfg.get("makemkv") or {}).get("makemkvcon_path") or "").strip()
        if mk and not Path(mk).exists():
            problems.append(f"config.json makemkv.makemkvcon_path not found: {mk}")
    except Exception:
        pass

    try:
        tvcfg = read_json(p["tv_config"])
        mk = (tvcfg.get("makemkv_path") or "").strip()
        if mk and not Path(mk).exists():
            problems.append(f"config_tv.json makemkv_path not found: {mk}")
    except Exception:
        pass

    if problems:
        raise RuntimeError("Health check failed:\n- " + "\n- ".join(problems))


def refresh_all() -> None:
    health_check()
    p = default_paths()
    py = sys.executable

    rc1 = run_cmd([py, str(app_dir() / "discmapper_v02.py"), "--config", str(p["movies_config"]),
                   "import-clz", "--clz", str(p["clz_csv"]), "--out", str(p["movies_index"])])
    if rc1 != 0:
        raise RuntimeError(f"Movies index refresh failed (exit {rc1}).")

    rc2 = run_cmd([py, str(app_dir() / "discmapper_tv_v02.py"),
                   "import-manifest", "--manifest", str(p["tv_manifest"]), "--out", str(p["tv_index"])])
    if rc2 != 0:
        raise RuntimeError(f"TV index refresh failed (exit {rc2}).")


def build_unified_queue() -> None:
    """
    Reuse proven queue builders (Movies GUI then TV GUI).
    """
    health_check()
    p = default_paths()
    py = sys.executable

    if not p["movies_index"].exists() or not p["tv_index"].exists():
        raise FileNotFoundError("Indexes missing. Run Refresh All Indexes first.")

    rc1 = run_cmd([py, str(app_dir() / "discmapper_v02.py"), "--config", str(p["movies_config"]),
                   "queue", "--index", str(p["movies_index"]), "--out", str(p["movies_queue"])])
    if rc1 != 0:
        raise RuntimeError(f"Movies queue build failed (exit {rc1}).")
    if not p["movies_queue"].exists():
        raise FileNotFoundError(
            f"Movies queue was not created: {p['movies_queue']}.\n"
            "In the Movies queue window, click Save & Close (or just close the window — it auto-saves if items exist)."
        )

    rc2 = run_cmd([py, str(app_dir() / "discmapper_tv_v02.py"),
                   "queue-builder", "--index", str(p["tv_index"]), "--out", str(p["tv_queue"])])
    if rc2 != 0:
        raise RuntimeError(f"TV queue build failed (exit {rc2}).")
    if not p["tv_queue"].exists():
        raise FileNotFoundError(
            f"TV queue was not created: {p['tv_queue']}.\n"
            "In the TV queue window, click Save Queue."
        )


def run_unified_queue() -> int:
    """
    Bulletproof run:
    - Movies first, then TV.
    - Non-zero from Movies does NOT prevent TV from running.
    """
    health_check()
    p = default_paths()
    py = sys.executable

    if not p["movies_queue"].exists():
        raise FileNotFoundError(f"Movies queue missing: {p['movies_queue']}")
    if not p["tv_queue"].exists():
        raise FileNotFoundError(f"TV queue missing: {p['tv_queue']}")
    if not p["tv_index"].exists():
        raise FileNotFoundError(f"TV index missing: {p['tv_index']}")

    rc_movies = run_cmd([py, str(app_dir() / "discmapper_v02.py"), "--config", str(p["movies_config"]),
                         "rip", "--queue", str(p["movies_queue"])])
    rc_tv = run_cmd([py, str(app_dir() / "discmapper_tv_v02.py"),
                     "rip-queue", "--index", str(p["tv_index"]), "--queue", str(p["tv_queue"]),
                     "--config", str(p["tv_config"])])

    if rc_movies != 0:
        return rc_movies
    return rc_tv


def gui() -> None:
    import tkinter as tk
    from tkinter import ttk, messagebox
    import threading

    migrate_config_paths()

    root = tk.Tk()
    root.title("DiscMapper UNIFIED v0.3 (Bulletproof)")
    root.geometry("940x560")

    frm = ttk.Frame(root, padding=16)
    frm.pack(fill="both", expand=True)

    ttk.Label(frm, text="DiscMapper UNIFIED v0.3", font=("Segoe UI", 18, "bold")).pack(anchor="w")
    ttk.Label(frm, text="One dashboard for Movies + TV. (Movies run first, then TV.)", font=("Segoe UI", 10)).pack(anchor="w", pady=(2, 14))

    status_var = tk.StringVar(value="Ready.")
    ttk.Label(frm, textvariable=status_var, font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 10))

    def set_status(s: str) -> None:
        status_var.set(s)
        root.update_idletasks()

    def run_bg(label: str, fn):
        def _worker():
            try:
                set_status(label)
                fn()
                set_status("Done.")
            except Exception as e:
                set_status("Error.")
                messagebox.showerror("Error", str(e))
        threading.Thread(target=_worker, daemon=True).start()

    ttk.Button(frm, text="Health Check", command=lambda: run_bg("Running health check...", health_check)).pack(fill="x", pady=6)
    ttk.Button(frm, text="Refresh All Indexes", command=lambda: run_bg("Refreshing indexes (Movies + TV)...", refresh_all)).pack(fill="x", pady=6)
    ttk.Button(frm, text="Build Unified Queue", command=lambda: run_bg("Building queues (Movies then TV)...", build_unified_queue)).pack(fill="x", pady=6)

    def on_run():
        def _run():
            rc = run_unified_queue()
            if rc != 0:
                raise RuntimeError(f"Run finished with non-zero exit code: {rc}\n(Queue still attempted Movies then TV.)")
        run_bg("Running queues (Movies then TV)...", _run)

    ttk.Button(frm, text="Run Unified Queue", command=on_run).pack(fill="x", pady=6)

    quick = ttk.LabelFrame(frm, text="Quick open folders")
    quick.pack(fill="x", pady=(18, 0))

    def open_folder(path: Path):
        try:
            path.mkdir(parents=True, exist_ok=True)
            subprocess.Popen(["explorer", str(path)])
        except Exception as e:
            messagebox.showerror("Error", str(e))

    r = root_dir()
    btnrow1 = ttk.Frame(quick); btnrow1.pack(fill="x", pady=(8,4))
    ttk.Button(btnrow1, text="Open Ready (Movies)", command=lambda: open_folder(r / "Staging" / "Movies" / "3_Ready")).pack(side="left", padx=(0, 8))
    ttk.Button(btnrow1, text="Open Ready (TV)", command=lambda: open_folder(r / "Staging" / "TV" / "3_Ready")).pack(side="left", padx=(0, 8))
    ttk.Button(btnrow1, text="Open Unable", command=lambda: open_folder(r / "Staging" / "Unable_to_Read")).pack(side="left")

    btnrow2 = ttk.Frame(quick); btnrow2.pack(fill="x", pady=(0,8))
    ttk.Button(btnrow2, text="Open Review (Movies)", command=lambda: open_folder(r / "Staging" / "Movies" / "2_Review")).pack(side="left", padx=(0, 8))
    ttk.Button(btnrow2, text="Open Review (TV)", command=lambda: open_folder(r / "Staging" / "TV" / "2_Review")).pack(side="left", padx=(0, 8))
    ttk.Button(btnrow2, text="Open Raw (Movies)", command=lambda: open_folder(r / "Staging" / "Movies" / "1_Raw")).pack(side="left", padx=(0, 8))
    ttk.Button(btnrow2, text="Open Raw (TV)", command=lambda: open_folder(r / "Staging" / "TV" / "1_Raw")).pack(side="left")

    root.mainloop()


def main() -> None:
    ap = argparse.ArgumentParser(prog="discmapper_unified_v03")
    ap.add_argument("cmd", nargs="?", default="gui", choices=["gui", "health", "refresh-all", "build-queue", "run"])
    args = ap.parse_args()

    if args.cmd == "gui":
        gui()
    elif args.cmd == "health":
        health_check()
    elif args.cmd == "refresh-all":
        refresh_all()
    elif args.cmd == "build-queue":
        build_unified_queue()
    elif args.cmd == "run":
        raise SystemExit(run_unified_queue())


if __name__ == "__main__":
    main()
