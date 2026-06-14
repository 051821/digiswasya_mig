"""
config/db.py
------------
Creates SQLAlchemy engines for the legacy and new app databases.
Connection strings are read from OLD_DB_URL and NEW_DB_URL in .env.
Accepts plain postgresql:// or postgres:// — rewrites to postgresql+psycopg2:// automatically.
"""

import os
from sqlalchemy import create_engine, Engine
from dotenv import load_dotenv

# Load .env file from the project root (one level up from config/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


def _fix_url(url: str) -> str:
    """
    Ensure the URL uses the postgresql+psycopg2:// scheme that SQLAlchemy requires.
    Handles:
      postgresql://...   -> postgresql+psycopg2://...
      postgres://...     -> postgresql+psycopg2://...   (common shorthand)
    Already-correct URLs are returned unchanged.
    """
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url  # already has driver specified (e.g. postgresql+psycopg2://)


def get_legacy_engine() -> Engine:
    """Engine for the old (source) PostgreSQL database."""
    url = _fix_url(os.environ["OLD_DB_URL"])
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=2)


def get_new_engine() -> Engine:
    """Engine for the new app (target) PostgreSQL database."""
    url = _fix_url(os.environ["NEW_DB_URL"])
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=2)