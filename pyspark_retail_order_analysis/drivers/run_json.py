import os
import pytest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
log_path = os.path.join(ROOT_DIR, "test_report.log")

if os.path.exists(log_path):
    os.remove(log_path)

pytest.main([
    os.path.join(ROOT_DIR, "tests", "test_from_config.py"),
    "-v",
    "--tb=short",
    "--disable-warnings"
])

