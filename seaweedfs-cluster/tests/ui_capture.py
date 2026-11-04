"""
Admin UI screenshot recorder for the Enterprise acceptance run.

Each capture() stores a full-page PNG plus a manifest entry (step, page, caption, time), which
docs/make_test_guide.py turns into the step-by-step test document.
"""
import json
import threading
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


class UICapture:
    def __init__(self, out_dir: Path, width=1440, height=900):
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self.dir / "manifest.json"
        self.items = json.loads(self.manifest.read_text()) if self.manifest.exists() else []
        self.lock = threading.Lock()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch()
        self._ctx = self._browser.new_context(viewport={"width": width, "height": height},
                                              device_scale_factor=1)

    def capture(self, base_url, path, step, caption, cluster="primary", full_page=True, settle=2.5):
        with self.lock:
            n = len(self.items) + 1
            name = f"{n:02d}-{cluster}-{path.strip('/').replace('/', '_') or 'dashboard'}.png"
            pg = self._ctx.new_page()
            try:
                pg.goto(base_url.rstrip("/") + path, wait_until="networkidle", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(settle)          # pages poll status via fetch(); let tiles fill
            try:
                pg.screenshot(path=str(self.dir / name), full_page=full_page)
            except Exception as e:  # noqa: BLE001
                pg.close()
                return None
            pg.close()
            self.items.append({"n": n, "file": name, "step": step, "page": path, "cluster": cluster,
                               "caption": caption, "time": time.strftime("%H:%M:%S")})
            self.manifest.write_text(json.dumps(self.items, indent=2))
            return name

    def close(self):
        try:
            self._browser.close()
            self._pw.stop()
        except Exception:  # noqa: BLE001
            pass
