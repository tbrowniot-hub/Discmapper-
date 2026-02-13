from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional


def _root_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def init_run_logger(script_name: str, mode: str, config_used: str, verbose: bool = False) -> logging.Logger:
    """Create a per-run timestamped logger in <root>/Logs/run_YYYYMMDD_HHMMSS.log."""
    logs_dir = _root_dir() / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"run_{ts}.log"

    logger = logging.getLogger(f"discmapper.{script_name}.{ts}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if verbose:
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    logger.info("Script start time: %s", datetime.now().isoformat(timespec="seconds"))
    logger.info("Mode selected: %s", mode)
    logger.info("Config file used: %s", config_used)
    logger.info("Log file: %s", log_path)
    return logger


def log_step(logger: Optional[logging.Logger], step_name: str, starting: bool = True) -> None:
    if not logger:
        return
    prefix = "START" if starting else "END"
    logger.info("%s | %s", prefix, step_name)
