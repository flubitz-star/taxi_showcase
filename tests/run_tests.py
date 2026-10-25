"""Run the unittest suite with the src package available on the import path."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test*.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
