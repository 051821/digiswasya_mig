"""
config/config.py
------------------
Non-connection migration settings, all read from environment / .env.
"""

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

LEGACY_SOURCE: str = os.environ.get("LEGACY_SOURCE", "legacy_v1")
BATCH_SIZE:    int  = int(os.environ.get("BATCH_SIZE", "500"))
DRY_RUN:       bool = os.environ.get("DRY_RUN", "false").lower() == "true"
LOG_LEVEL:     str  = os.environ.get("LOG_LEVEL", "INFO").upper()
PREVIEW_SAMPLE_SIZE: int = int(os.environ.get("PREVIEW_SAMPLE_SIZE", "5"))

if __name__ == "__main__":
    print(f"LEGACY_SOURCE: {LEGACY_SOURCE}")
    print(f"BATCH_SIZE: {BATCH_SIZE}")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"LOG_LEVEL: {LOG_LEVEL}")