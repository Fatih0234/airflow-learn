from __future__ import annotations

import requests
from airflow.sdk import dag, task, get_current_context
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://jsonplaceholder.typicode.com"


@dag(
    dag_id="jsonplaceholder_posts_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "etl", "jsonplaceholder"],
)
def jsonplaceholder_posts_etl():

    @task
    def ensure_tables() -> None:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        ddl =   """
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.posts (
                    id          INTEGER PRIMARY KEY,
                    user_id     INTEGER NOT NULL,
                    title       TEXT,
                    body        TEXT,

                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_run_id TEXT NOT NULL
                );

                CREATE SCHEMA IF NOT EXISTS audit;


                CREATE TABLE IF NOT EXISTS audit.etl_run_audit (
                    id          BIGSERIAL PRIMARY KEY,
                    dag_id      TEXT NOT NULL,
                    run_id      TEXT NOT NULL,  
                    task_id     TEXT NOT NULL,
                    attempted   INTEGER NOT NULL,
                    touched     INTEGER NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
        
        hook.run(ddl)

    @task
    def extract_posts() -> list[dict]:
        url = f"{API_BASE}/posts"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        print(f"Fetched {len(data)} posts from {url}")
        return data
    
    @task
    def transform_posts(posts: list[dict]) -> list[dict]:
        out: list[dict] = []
        for p in posts:
            out.append(
                {
                    "id": p["id"],
                    "user_id": p.get("userId"),
                    "title": p.get("title"),
                    "body": p.get("body"),
                }
            )
        print(f"Transformed into {len(out)} rows")
        return out
    
    @task
    def load_posts(rows: list[dict]) -> dict:
        ctx = get_current_context()
        run_id = ctx["run_id"]
        dag_id = ctx["dag"].dag_id

        for r in rows:
            r["run_id"] = run_id

        hook = PostgresHook(postgres_conn_id="app_postgres")
        sql = """
            INSERT INTO raw.posts (id, user_id, title, body, ingested_at, last_run_id)
            VALUES(%(id)s, %(user_id)s, %(title)s, %(body)s, now(), %(run_id)s)
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
                cur.execute("SELECT count(*) FROM raw.posts WHERE last_run_id=%s;", (run_id,))
                touched = int(cur.fetchone()[0])

            conn.commit()

        finally:
            conn.close()

        attempted = len(rows)
        print(f"Attempted: {attempted}, touched_this_run: {touched}, run_id: {run_id}")
        return {"dag_id": dag_id, "run_id": run_id, "attempted": attempted, "touched": touched}
    
    @task
    def write_audit(metrics: dict) -> None:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        ctx = get_current_context()
        task_id = ctx["task"].task_id
        
        hook.run(
            """
            INSERT INTO audit.etl_run_audit (dag_id, run_id, task_id, attempted, touched)
            VALUES (%s, %s, %s, %s, %s)
            """,
            parameters=(metrics["dag_id"], metrics["run_id"], task_id, metrics["attempted"], metrics["touched"]),
        )

    @task
    def validate_run(metrics: dict) -> None:
        if metrics["touched"] != metrics["attempted"]:
            raise ValueError(f"Validation failed: touched {metrics['touched']} != attempted {metrics['attempted']}")
        print("Run validation OK")

    ensure = ensure_tables()
    posts = extract_posts()
    rows = transform_posts(posts)
    metrics = load_posts(rows)
    audit = write_audit(metrics)
    check = validate_run(metrics)

    ensure >> posts >> rows >> metrics >> audit >> check


jsonplaceholder_posts_etl()