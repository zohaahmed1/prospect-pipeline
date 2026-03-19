#!/usr/bin/env python3
"""Upwork Job Researcher + Proposal Generator — launcher."""

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    app_path = Path(__file__).parent / "upwork" / "app.py"
    subprocess.run(["streamlit", "run", str(app_path)] + sys.argv[1:])
