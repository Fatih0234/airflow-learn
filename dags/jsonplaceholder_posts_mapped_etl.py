from __future__ import annotations

import requests
from airflow.sdk import dag, task, get_current_context
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://jsonplaceholder.typicode.com"


@dag(
    dag_id="jsonplaceholder_posts_mapped_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "etl", "jsonplaceholder", "mapping"],
)
def jsonplaceholder_posts_mapped_etl():

    @task
    def ensure_tables() -> None:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        ddl = """
        CREATE SCHEMA IF NOT EXISTS raw;

        CREATE TABLE IF NOT EXISTS raw.posts (
            id          INTEGER PRIMARY KEY,
            user_id     INTEGER NOT NULL,
            title       TEXT,
            body        TEXT,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_run_id TEXT NOT NULL
        );
        """
        hook.run(ddl)

    @task
    def get_user_ids() -> list[int]:
        r = requests.get(f"{API_BASE}/users", timeout=30)
        r.raise_for_status()
        users = r.json()
        user_ids = [u["id"] for u in users]
        print(f"Got {len(user_ids)} user_ids")
        return user_ids

    @task
    def fetch_posts_for_user(user_id: int) -> list[dict]:
        # each mapped task handles one user
        r = requests.get(f"{API_BASE}/posts", params={"userId": user_id}, timeout=30)
        r.raise_for_status()
        data = r.json()
        print(f"user_id={user_id} -> {len(data)} posts")
        return data

    @task
    def flatten_posts(posts_lists: list[list[dict]]) -> list[dict]:
        # posts_lists is list of lists (one list per user_id)
        out: list[dict] = []
        for posts in posts_lists:
            for p in posts:
                out.append(
                    {
                        "id": p["id"],
                        "user_id": p.get("userId"),
                        "title": p.get("title"),
                        "body": p.get("body"),
                    }
                )
        print(f"Flattened into {len(out)} total posts")
        return out

    @task
    def load_posts(rows: list[dict]) -> dict:
        ctx = get_current_context()
        run_id = ctx["run_id"]
        for r in rows:
            r["run_id"] = run_id

        hook = PostgresHook(postgres_conn_id="app_postgres")
        sql = """
        INSERT INTO raw.posts (id, user_id, title, body, ingested_at, last_run_id)
        VALUES (%(id)s, %(user_id)s, %(title)s, %(body)s, now(), %(run_id)s)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            title = EXCLUDED.title,
            body = EXCLUDED.body,
            ingested_at = now(),
            last_run_id = EXCLUDED.last_run_id;
        """

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
                cur.execute("SELECT count(*) FROM raw.posts WHERE last_run_id = %s;", (run_id,))
                touched = int(cur.fetchone()[0])
            conn.commit()
        finally:
            conn.close()

        attempted = len(rows)
        print(f"Attempted: {attempted}, touched_this_run: {touched}")
        return {"attempted": attempted, "touched": touched}

    @task
    def validate_run(metrics: dict) -> None:
        if metrics["touched"] != metrics["attempted"]:
            raise ValueError(f"Validation failed: touched {metrics['touched']} != attempted {metrics['attempted']}")
        print("Run validation OK")

    ensure = ensure_tables()
    user_ids = get_user_ids()

    # Dynamic mapping: one task instance per user_id
    posts_per_user = fetch_posts_for_user.expand(user_id=user_ids)

    rows = flatten_posts(posts_per_user)
    metrics = load_posts(rows)
    check = validate_run(metrics)

    ensure >> user_ids >> posts_per_user >> rows >> metrics >> check


jsonplaceholder_posts_mapped_etl()