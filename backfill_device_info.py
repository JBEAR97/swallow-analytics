from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


TABLE_NAME = '"swallow-analysis"'
BATCH_SIZE = 1000


def detect_device_context(user_agent: str) -> tuple[str, str, str]:
    ua = user_agent.lower()

    if any(token in ua for token in ("ipad", "tablet")):
        device_category = "tablet"
    elif any(token in ua for token in ("mobile", "iphone", "android")):
        device_category = "mobile"
    else:
        device_category = "desktop"

    if "edg/" in ua:
        browser = "Edge"
    elif "opr/" in ua or "opera" in ua:
        browser = "Opera"
    elif "chrome/" in ua and "edg/" not in ua:
        browser = "Chrome"
    elif "firefox/" in ua:
        browser = "Firefox"
    elif "safari/" in ua and "chrome/" not in ua:
        browser = "Safari"
    else:
        browser = "Other"

    if "windows" in ua:
        operating_system = "Windows"
    elif "iphone" in ua or "ipad" in ua or "ios" in ua:
        operating_system = "iOS"
    elif "android" in ua:
        operating_system = "Android"
    elif "mac os x" in ua or "macintosh" in ua:
        operating_system = "macOS"
    elif "linux" in ua:
        operating_system = "Linux"
    else:
        operating_system = "Other"

    return device_category, browser, operating_system


def fetch_batch(conn) -> list[dict]:
    result = conn.execute(
        text(
            f"""
            SELECT id, COALESCE(user_agent, '') AS user_agent
            FROM {TABLE_NAME}
            WHERE
                COALESCE(device_category, '') = ''
                OR COALESCE(browser, '') = ''
                OR COALESCE(operating_system, '') = ''
            ORDER BY id ASC
            LIMIT :limit
            """
        ),
        {"limit": BATCH_SIZE},
    )
    return [dict(row._mapping) for row in result]


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    engine = create_engine(database_url, pool_pre_ping=True)
    total_updated = 0

    while True:
        with engine.begin() as conn:
            rows = fetch_batch(conn)
            if not rows:
                break

            for row in rows:
                device_category, browser, operating_system = detect_device_context(row["user_agent"])
                conn.execute(
                    text(
                        f"""
                        UPDATE {TABLE_NAME}
                        SET
                            device_category = :device_category,
                            browser = :browser,
                            operating_system = :operating_system
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "device_category": device_category,
                        "browser": browser,
                        "operating_system": operating_system,
                    },
                )

            total_updated += len(rows)
            print(f"Updated {total_updated} rows...")

    print(f"Done. Backfilled {total_updated} rows.")


if __name__ == "__main__":
    main()
