from airflow.sdk import task, dag
from time import sleep

@dag
def celery_task():
    @task
    def a():
        sleep(5)

    @task(
            queue="high_cpu"
    )
    def b():
        sleep(5)

    @task(
            queue="high_cpu"
    )
    def c():
        sleep(5)

    @task(
            queue="high_cpu"
    )
    def d():
        sleep(5)

    a() >> [b(), c()] >> d()

celery_task()
