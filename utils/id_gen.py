import uuid
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


class SafeIDGenerator:
    """
    Generates UUID4s that are guaranteed not to clash with existing rows
    in the target table.

    Usage
    -----
        gen = SafeIDGenerator(new_engine, table="patient")
        new_uuid = gen.next()
    """

    def __init__(self, engine: Engine, table: str, id_column: str = "id"):
        self._used: set[str] = set()
        self._load_existing(engine, table, id_column)

    def _load_existing(self, engine: Engine, table: str, id_column: str) -> None:
        log.info("Loading existing IDs from '%s.%s' …", table, id_column)
        with engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT {id_column}::text FROM "{table}"')
            ).fetchall()
        self._used = {str(r[0]) for r in rows}
        log.info("  → %d existing IDs loaded.", len(self._used))

    def next(self) -> str:
        """Return a brand-new UUID4 string not present in the target table."""
        while True:
            candidate = str(uuid.uuid4())
            if candidate not in self._used:
                self._used.add(candidate)   # reserve it immediately
                return candidate


def get_migrated_legacy_ids(engine: Engine, table: str) -> set[str]:
    """
    Return the set of legacy_id values already present in `table`.
    Used to skip rows that were already migrated in a previous run.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(f'SELECT legacy_id FROM "{table}" WHERE legacy_id IS NOT NULL')
        ).fetchall()
    return {str(r[0]) for r in rows}