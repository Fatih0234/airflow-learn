from __future__ import annotations

import requests
from airflow.sdk import dag, task, get_current_context
from pendulum import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = "https://jsonplaceholder.typicode.com"


@dag(
    dag_id="jsonplaceholder_users_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "etl", "jsonplaceholder"],
)
def jsonplaceholder_users_etl():
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

        CREATE SCHEMA IF NOT EXISTS audit;

        CREATE TABLE IF NOT EXISTS audit.etl_run_audit (
            id          BIGSERIAL PRIMARY KEY,
            dag_id       TEXT NOT NULL,
            run_id       TEXT NOT NULL,
            task_id      TEXT NOT NULL,
            attempted    INTEGER NOT NULL,
            touched      INTEGER NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        hook.run(ddl)

    @task
    def extract_users() -> list[dict]:
        url = f"{API_BASE}/users"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        print(f"Fetched {len(data)} users from {url}")
        return data

    @task
    def transform_users(users: list[dict]) -> list[dict]:
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

        print(f"Transformed into {len(out)} rows")
        return out

    @task
    def load_users(rows: list[dict]) -> dict:
        """
        Upsert users, stamp rows with last_run_id.
        Returns run metrics: attempted + touched_this_run.
        """
        ctx = get_current_context()
        run_id = ctx["run_id"]
        dag_id = ctx["dag"].dag_id

        # attach run_id to each row for insert/upsert
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

                # how many rows were touched by *this* run?
                cur.execute("SELECT count(*) FROM raw.users WHERE last_run_id = %s;", (run_id,))
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
            parameters=(
                metrics["dag_id"],
                metrics["run_id"],
                task_id,
                metrics["attempted"],
                metrics["touched"],
            ),
        )

    @task
    def validate_run(metrics: dict) -> None:
        """
        Simple run-scoped correctness check:
        For this dataset, we expect the run to touch exactly what it attempted.
        """
        attempted = metrics["attempted"]
        touched = metrics["touched"]
        if touched != attempted:
            raise ValueError(f"Run validation failed: touched {touched} != attempted {attempted}")
        print(f"Run validation OK: touched {touched} == attempted {attempted}")

    # Wiring
    ensure = ensure_tables()
    users = extract_users()
    rows = transform_users(users)
    metrics = load_users(rows)
    audit = write_audit(metrics)
    check = validate_run(metrics)

    ensure >> users >> rows >> metrics >> audit >> check


jsonplaceholder_users_etl()