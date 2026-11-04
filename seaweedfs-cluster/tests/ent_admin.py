"""Re-export of bin/seaweed_admin_api.py for the test suites."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
from seaweed_admin_api import AdminAPI  # noqa: E402,F401
