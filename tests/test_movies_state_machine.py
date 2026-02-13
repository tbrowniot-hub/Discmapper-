import io
import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path

import importlib.util
import sys

APP_DIR = Path(__file__).resolve().parents[1] / "DiscMapper_unified" / "App"
sys.path.insert(0, str(APP_DIR))
MODULE_PATH = Path(__file__).resolve().parents[1] / "DiscMapper_unified" / "App" / "discmapper_v02.py"
spec = importlib.util.spec_from_file_location("discmapper_v02", MODULE_PATH)
discmapper_v02 = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules["discmapper_v02"] = discmapper_v02
spec.loader.exec_module(discmapper_v02)


class MoviesStateMachineTests(unittest.TestCase):
    def _write_json(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")

    def test_dry_run_state_order(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "App" / "config.json"
            queue_path = root / "Data" / "Queues" / "queue.json"

            cfg = dict(discmapper_v02.DEFAULT_CONFIG)
            cfg["raw_root"] = str(root / "Staging" / "Movies" / "1_Raw")
            cfg["done_root"] = str(root / "Staging" / "Movies" / "1_Raw" / "_done")
            cfg["ready_root"] = str(root / "Staging" / "Movies" / "3_Ready")
            cfg["workbench_review"] = str(root / "Staging" / "Movies" / "2_Review")
            cfg["workbench_unable"] = str(root / "Staging" / "Unable_to_Read")
            cfg["eject"] = {"enabled": False, "drive_letter": ""}
            self._write_json(cfg_path, cfg)

            self._write_json(queue_path, {
                "items": [{"title": "Test Movie", "year": 2024, "imdb_id": "tt1234567", "index": 1, "format": "Blu-ray"}]
            })

            args = Namespace(config=str(cfg_path), queue=str(queue_path), dry_run=True)
            out = io.StringIO()
            with redirect_stdout(out):
                discmapper_v02.cmd_rip(args)
            text = out.getvalue()

            self.assertIn("WAIT_FOR_DISC -> DISC_DETECTED -> RIP -> VERIFY_OUTPUTS -> PLAN_RENAME -> COMMIT_MOVES -> DONE", text)

    def test_timing_config_is_loaded(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = dict(discmapper_v02.DEFAULT_CONFIG)
            cfg["timing"] = {
                "poll_interval_seconds": 9,
                "disc_settle_seconds": 7,
                "post_rip_settle_seconds": 6,
                "eject_delay_seconds": 5,
                "max_wait_minutes": 2,
            }
            timing, _policy = discmapper_v02.load_timing_policy(cfg)
            self.assertEqual(9, timing["poll_interval_seconds"])
            self.assertEqual(2, timing["max_wait_minutes"])


if __name__ == "__main__":
    unittest.main()
