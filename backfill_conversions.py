from __future__ import annotations

import os

from sqlalchemy import bindparam, create_engine, text

try:
    from dotenv import load_dotenv
except ImportError:  # Railway runtime may not include python-dotenv
    load_dotenv = None


TABLE_NAME = '"swallow-analysis"'
BATCH_SIZE = 1000
DEFAULT_ACTION_TYPES = ("cta_click",)
DEFAULT_EVENT_TYPES: tuple[str, ...] = ()


def parse_csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def build_match_query(action_types: tuple[str, ...], event_types: tuple[str, ...]):
    conditions: list[str] = []
    params: dict[str, object] = {"limit": BATCH_SIZE}

    if action_types:
        conditions.append("COALESCE(action_type, '') IN :action_types")
        params["action_types"] = action_types
    if event_types:
        conditions.append("event_type IN :event_types")
        params["event_types"] = event_types

    if not conditions:
        raise RuntimeError("No conversion rules configured")

    query = text(
        f"""
        SELECT
            id,
            event_type,
            action_type,
            COALESCE(conversion_name, '') AS conversion_name
        FROM {TABLE_NAME}
        WHERE COALESCE(is_conversion, FALSE) = FALSE
          AND ({' OR '.join(conditions)})
        ORDER BY id ASC
        LIMIT :limit
        """
    )

    if action_types:
        query = query.bindparams(bindparam("action_types", expanding=True))
    if event_types:
        query = query.bindparams(bindparam("event_types", expanding=True))

    return query, params


def fetch_batch(conn, action_types: tuple[str, ...], event_types: tuple[str, ...]) -> list[dict]:
    query, params = build_match_query(action_types, event_types)
    result = conn.execute(query, params)
    return [dict(row._mapping) for row in result]


def infer_conversion_name(row: dict) -> str:
    if row["conversion_name"]:
        return row["conversion_name"]
    return row["action_type"] or row["event_type"]


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    action_types = parse_csv_env("CONVERSION_ACTION_TYPES", DEFAULT_ACTION_TYPES)
    event_types = parse_csv_env("CONVERSION_EVENT_TYPES", DEFAULT_EVENT_TYPES)

    print(f"Using action_type rules: {', '.join(action_types) if action_types else '(none)'}")
    print(f"Using event_type rules: {', '.join(event_types) if event_types else '(none)'}")

    engine = create_engine(database_url, pool_pre_ping=True)
    total_updated = 0

    while True:
        with engine.begin() as conn:
            rows = fetch_batch(conn, action_types, event_types)
            if not rows:
                break

            for row in rows:
                conn.execute(
                    text(
                        f"""
                        UPDATE {TABLE_NAME}
                        SET
                            is_conversion = TRUE,
                            conversion_name = :conversion_name
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "conversion_name": infer_conversion_name(row),
                    },
                )

            total_updated += len(rows)
            print(f"Updated {total_updated} rows...")

    print(f"Done. Backfilled {total_updated} conversion rows.")


if __name__ == "__main__":
    main()
