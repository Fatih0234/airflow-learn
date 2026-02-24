from __future__ import annotations

import requests
from airflow.sdk import dag, task, task_group, get_current_context
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://jsonplaceholder.typicode.com"


@dag(
    dag_id="jsonplaceholder_pipeline_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "etl", "jsonplaceholder", "taskgroup"],
)
def jsonplaceholder_pipeline_etl():

    # ---------- shared helpers ----------
    @task
    def ensure_audit_table() -> None:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        ddl = """
        CREATE SCHEMA IF NOT EXISTS audit;

        CREATE TABLE IF NOT EXISTS audit.etl_run_audit (
            id         BIGSERIAL PRIMARY KEY,
            dag_id     TEXT NOT NULL,
            run_id     TEXT NOT NULL,
            task_id    TEXT NOT NULL,
            attempted  INTEGER NOT NULL,
            touched    INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        hook.run(ddl)

    @task
    def write_audit(entity: str, metrics: dict) -> None:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        ctx = get_current_context()
        dag_id = ctx["dag"].dag_id
        run_id = ctx["run_id"]
        task_id = f"{entity}.{ctx['task'].task_id}"

        hook.run(
            """
            INSERT INTO audit.etl_run_audit (dag_id, run_id, task_id, attempted, touched)
            VALUES (%s, %s, %s, %s, %s)
            """,
            parameters=(dag_id, run_id, task_id, metrics["attempted"], metrics["touched"]),
        )

    @task
    def validate_run(entity: str, metrics: dict) -> None:
        if metrics["touched"] != metrics["attempted"]:
            raise ValueError(
                f"[{entity}] validation failed: touched {metrics['touched']} != attempted {metrics['attempted']}"
            )
        print(f"[{entity}] validation OK")

    # ---------- USERS group ----------
    @task_group(group_id="users")
    def users_group():
        @task
        def ensure_tables() -> None:
            hook = PostgresHook(postgres_conn_id="app_postgres")
            ddl = """
            CREATE SCHEMA IF NOT EXISTS raw;

            CREATE TABLE IF NOT EXISTS raw.users (
                id                  INTEGER PRIMARY KEY,
                name                TEXT,
                username            TEXT,
                email               TEXT,
                phone               TEXT,
                website             TEXT,

                address_street      TEXT,
                address_suite       TEXT,
                address_city        TEXT,
                address_zipcode     TEXT,
                address_lat         TEXT,
                address_lng         TEXT,

                company_name        TEXT,
                company_catchphrase TEXT,
                company_bs          TEXT,

                ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_run_id         TEXT NOT NULL
            );
            """
            hook.run(ddl)

        @task
        def extract() -> list[dict]:
            url = f"{API_BASE}/users"
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"Fetched {len(data)} users")
            return data

        @task
        def transform(users: list[dict]) -> list[dict]:
            out: list[dict] = []
            for u in users:
                addr = u.get("address") or {}
                geo = addr.get("geo") or {}
                comp = u.get("company") or {}

                out.append(
                    {
                        "id": u["id"],
                        "name": u.get("name"),
                        "username": u.get("username"),
                        "email": u.get("email"),
                        "phone": u.get("phone"),
                        "website": u.get("website"),
                        "address_street": addr.get("street"),
                        "address_suite": addr.get("suite"),
                        "address_city": addr.get("city"),
                        "address_zipcode": addr.get("zipcode"),
                        "address_lat": geo.get("lat"),
                        "address_lng": geo.get("lng"),
                        "company_name": comp.get("name"),
                        "company_catchphrase": comp.get("catchPhrase"),
                        "company_bs": comp.get("bs"),
                    }
                )
            return out

        @task
        def load(rows: list[dict]) -> dict:
            ctx = get_current_context()
            run_id = ctx["run_id"]

            for r in rows:
                r["run_id"] = run_id

            hook = PostgresHook(postgres_conn_id="app_postgres")
            sql = """
            INSERT INTO raw.users (
                id, name, username, email, phone, website,
                address_street, address_suite, address_city, address_zipcode, address_lat, address_lng,
                company_name, company_catchphrase, company_bs,
                ingested_at, last_run_id
            )
            VALUES (
                %(id)s, %(name)s, %(username)s, %(email)s, %(phone)s, %(website)s,
                %(address_street)s, %(address_suite)s, %(address_city)s, %(address_zipcode)s, %(address_lat)s, %(address_lng)s,
                %(company_name)s, %(company_catchphrase)s, %(company_bs)s,
                now(), %(run_id)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                website = EXCLUDED.website,
                address_street = EXCLUDED.address_street,
                address_suite = EXCLUDED.address_suite,
                address_city = EXCLUDED.address_city,
                address_zipcode = EXCLUDED.address_zipcode,
                address_lat = EXCLUDED.address_lat,
                address_lng = EXCLUDED.address_lng,
                company_name = EXCLUDED.company_name,
                company_catchphrase = EXCLUDED.company_catchphrase,
                company_bs = EXCLUDED.company_bs,
                ingested_at = now(),
                last_run_id = EXCLUDED.last_run_id;
            """

            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    cur.execute("SELECT count(*) FROM raw.users WHERE last_run_id = %s;", (run_id,))
                    touched = int(cur.fetchone()[0])
                conn.commit()
            finally:
                conn.close()

            attempted = len(rows)
            return {"attempted": attempted, "touched": touched}

        t0 = ensure_tables()
        u = extract()
        r = transform(u)
        m = load(r)
        a = write_audit("users", m)
        v = validate_run("users", m)

        t0 >> u >> r >> m >> a >> v
        return v  # group "completion" handle

    # ---------- POSTS group ----------
    @task_group(group_id="posts")
    def posts_group():
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
        def extract() -> list[dict]:
            url = f"{API_BASE}/posts"
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"Fetched {len(data)} posts")
            return data

        @task
        def transform(posts: list[dict]) -> list[dict]:
            return [{"id": p["id"], "user_id": p.get("userId"), "title": p.get("title"), "body": p.get("body")} for p in posts]

        @task
        def load(rows: list[dict]) -> dict:
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
            return {"attempted": attempted, "touched": touched}

        t0 = ensure_tables()
        p = extract()
        r = transform(p)
        m = load(r)
        a = write_audit("posts", m)
        v = validate_run("posts", m)

        t0 >> p >> r >> m >> a >> v
        return v

    # ---------- COMMENTS group ----------
    @task_group(group_id="comments")
    def comments_group():
        @task
        def ensure_tables() -> None:
            hook = PostgresHook(postgres_conn_id="app_postgres")
            ddl = """
            CREATE SCHEMA IF NOT EXISTS raw;

            CREATE TABLE IF NOT EXISTS raw.comments (
                id          INTEGER PRIMARY KEY,
                post_id     INTEGER NOT NULL,
                name        TEXT,
                email       TEXT,
                body        TEXT,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_run_id TEXT NOT NULL
            );
            """
            hook.run(ddl)

        @task
        def extract() -> list[dict]:
            url = f"{API_BASE}/comments"
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"Fetched {len(data)} comments")
            return data

        @task
        def transform(comments: list[dict]) -> list[dict]:
            return [
                {"id": c["id"], "post_id": c.get("postId"), "name": c.get("name"), "email": c.get("email"), "body": c.get("body")}
                for c in comments
            ]

        @task
        def load(rows: list[dict]) -> dict:
            ctx = get_current_context()
            run_id = ctx["run_id"]
            for r in rows:
                r["run_id"] = run_id

            hook = PostgresHook(postgres_conn_id="app_postgres")
            sql = """
            INSERT INTO raw.comments (id, post_id, name, email, body, ingested_at, last_run_id)
            VALUES (%(id)s, %(post_id)s, %(name)s, %(email)s, %(body)s, now(), %(run_id)s)
            ON CONFLICT (id) DO UPDATE SET
                post_id = EXCLUDED.post_id,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                body = EXCLUDED.body,
                ingested_at = now(),
                last_run_id = EXCLUDED.last_run_id;
            """

            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    cur.execute("SELECT count(*) FROM raw.comments WHERE last_run_id = %s;", (run_id,))
                    touched = int(cur.fetchone()[0])
                conn.commit()
            finally:
                conn.close()

            attempted = len(rows)
            return {"attempted": attempted, "touched": touched}

        t0 = ensure_tables()
        c = extract()
        r = transform(c)
        m = load(r)
        a = write_audit("comments", m)
        v = validate_run("comments", m)

        t0 >> c >> r >> m >> a >> v
        return v

    # ---------- DAG wiring ----------
    audit_ready = ensure_audit_table()
    users_done = users_group()
    posts_done = posts_group()
    comments_done = comments_group()

    audit_ready >> users_done >> posts_done >> comments_done


jsonplaceholder_pipeline_etl()