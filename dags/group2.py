from airflow.sdk import dag, task, task_group
from pendulum import datetime

@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def group():

    @task
    def a():
        print("a")

    @task_group(default_args={"retries": 2})
    def my_group():

        @task
        def b():
            return 42

        @task_group(default_args={"retries": 3})
        def my_nested_group(val:int):
            @task
            def c(my_val: int):
                print(my_val)

            # use the outer-scope XComArg here
            c(my_val=val)

        val2 = b()          # XComArg (ok)
        my_nested_group(val2)  # no runtime argument into TaskGroup

    a() >> my_group()

group()


from airflow.sdk import dag, task, task_group
from pendulum import datetime

@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def group2():

    @task
    def a():
        print("a")

    @task_group(default_args={"retries": 2})
    def my_group():

        @task
        def b():
            return 42

        @task_group(default_args={"retries": 3})
        def my_nested_group():
            @task
            def c(my_val: int):
                print(my_val)

            # use the outer-scope XComArg here
            c(my_val=val)

        val = b()          # XComArg (ok)
        my_nested_group()  # no runtime argument into TaskGroup

    a() >> my_group()

group2()