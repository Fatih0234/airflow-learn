from airflow.sdk import dag, task, Context
from typing import Dict, Any




@dag
def xcom_dag():

    @task
    def t1() -> Dict[int, Any]:
        val = 42
        first_word = "Hello, there!"
        return {
            "nmb" : val,
            "fw": first_word
        }

    @task
    def t2(val: Dict[int, Any]):
        print(val["nmb"])
        print(val["fw"])

    
    val = t1()
    t2(val)

xcom_dag()