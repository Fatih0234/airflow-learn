from __future__ import annotations

from airflow.sdk import dag, task
from pendulum import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="app_db_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "postgres", "smoke"],
)
def app_db_smoke_test():
    """
    Smoke test: can Airflow reach the *app* Postgres (postgres-2) via connection `app_postgres`?
    """

    @task
    def select_one() -> int:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        val = hook.get_first("Select 1;")[0]
        print(f"SELECT 1 returned: {val}")
        return int(val)
    
    @task
    def select_now() -> str:
        hook = PostgresHook(postgres_conn_id="app_postgres")
        now = hook.get_first("SELECT now();")[0]
        print(f"DB time is: {now}")
        return str(now)
    
    select_one() >> select_now()

app_db_smoke_test()