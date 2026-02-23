from airflow.sdk import task, dag


@dag
def branch():

    @task
    def a():
        return 5

    @task.branch
    def b(val: int):

        if val == 1:
            return "equal_1"
        return "different_1"
    
    @task
    def equal_1(val: int):
        print(f"equal {val}")
    
    @task    
    def different_1(val: int):
        print(f"different than 1: {val}")

    val = a()
    b(val) >> [equal_1(val), different_1(val)]


branch()