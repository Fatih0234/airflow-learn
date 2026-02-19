from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql="""
                CREATE TABLE IF NO EXISTS users (
                    id INT PRIMARY KEY, 
                    firstname VARCHAR(255),
                    lastname VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMPT
                )
            """
    )


user_processing() # otherwise it doesnt show up in dah console.
