from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



@dag
def user_processing():    

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS users (
                    id INT PRIMARY KEY, 
                    firstname VARCHAR(255),
                    lastname VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        r = requests.get("https://jsonplaceholder.typicode.com/users", timeout=10)
        if r.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=r.json()[0])
        return PokeReturnValue(is_done=False)

    @task
    def extract_user(fake_user: dict) -> dict:
        if not fake_user:
        # local test fallback
            fake_user = {"id": 1, "name": "Leanne Graham", "email": "leanne@example.com"}
        name_parts = fake_user["name"].split()
        return {
            "id": int(fake_user["id"]),
            "firstname": name_parts[0],
            "lastname": name_parts[-1],
            "email": fake_user["email"],
        }
    
    @task
    def process_user(user_info: dict):
        import csv
        import os
        from datetime import datetime


        path = "/tmp/user_info.csv"
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # 1) make sure folder exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # 2) write header if file doesn't exist, then append row
        file_exists = os.path.exists(path)

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(sql="COPY users FROM STDIN WITH CSV HEADER",
                         filename="/tmp/user_info.csv")
    

    process_user(extract_user(create_table >> is_api_available())) >> store_user()


user_processing() # otherwise it doesnt show up in dah console.
