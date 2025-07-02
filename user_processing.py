from airflow.sdk import dag, task # @dag tells airflow that this function creates a workflow, @task turns a regular python function into a task that airflow can run
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # let's you run SQL commands (CREATE TABLE) airflow's way of sending SQL to your database
from airflow.sdk.bases.sensor import PokeReturnValue # Used when making a sensor (a task that is waiting for something like a file/ API)
from airflow.providers.postgres.hooks.postgres import PostgresHook # hook allows you to interact with progres database, gives you access to certain things that the operator does not give you access to
# from airflow.providers.standard.operators.python import PythonOperator # Let's you run any custom Python code as a task, we can remove this line and use the first line above. As @TASK is a python operator
# from airflow.decorators import dag, task
#from airflow.sensors.base import PokeReturnValue

# DEFINE PYTHON FUNCTION, DAG(WORKFLOW) is UNIQUE TO create TASK create_table
@dag # use DAG decorator
def user_processing(): # Hey, this function defines a DAG (workflow), not just a regular Python function.
    # now create a task, so create a variable
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table", # unique identifier of your task in this DAG, anme of the task you see on Airflow
        conn_id = "postgres", # unique identifier to your postgres database
        sql ="""
        CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""" # in addititon to task_id, you have a few more parameters specific to SQL ExecuteOperator, to start with, the sql query that you want to run in your postgress database
    )
# in order to create a table in a postgres database we need to run a sql operator
# postrgress operator = template for a predefined task but this is specific to postgress
# so we use the SQL OPERATOR - SQLExecuteQueryOperator as it's more general
# to test it works go to Docker Desktop > click on the scheduler container > /bin/bash > airflow tasks test <DAG function> <task> i.e airflow tasks test user_processing create_table

# Implement a SENSOR: Check if API_IS_AVAIALBLE using a SENSOR - OPERATOR allowing you to wait if a condition is TRUE before proceeding to next task -> need to import aanother decorator 
    @task.sensor(poke_interval=30, timeout=300) # create another task to verify if the API is available or not. use the task decorator.sensor to indicate that the python function is a sensor
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done = condition, xcom_value=fake_user) # Stores the result (fake_user JSON) so that other tasks can access it later
   

    # XTRACT USER, EXTRACT DATA without xcoms for the sake of the TEST, this fetches data directly from the API
    @task
    def extract_user(fake_user): #ti = task instance object to fetch data from a previous task, is_api_available
        return {
            "id":fake_user["id"],
            "firstname":fake_user["personalInfo"]["firstName"],
            "lastname":fake_user["personalInfo"]["lastName"],
            "email":fake_user["personalInfo"]["email"]
    }
    
    # PROCESS USER
    @task
    def process_user(user_info): # process user info and save to a csv as '/tmp/user_info.csv'
        import csv
        from datetime import datetime
        user_info = {
            "id":"123",
            "firstname": "John",
            "lastname":"Doe",
            "email":"john.doe@example.com"
        }
        user_info["create_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # added after process_user 
        # store as csv: user_info.csv should be stored in the /tmp folder
        with open('/tmp/user_info.csv', 'w',newline='') as f:
            writer = csv.DictWriter(f,fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    
    # CLEAN USER TABLE
    # @task
    # def clear_users_table():
    #     hook = PostgresHook(postgres_conn_id="postgres")
    #     hook.run("DELETE FROM users;")

    # STORE USER - HOOK that COPIES over /tmp/user_info.csv & LOADS the data into the database
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres") # create the hook variable that takes the connection that you created to connect to postgres database
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER", #users table
            filename="/tmp/user_info.csv"
        ) # hook

    # process_user(extract_user(create_table >> is_api_available())) >> store_user()
    fake_user = is_api_available()
    user_info = extract_user(fake_user) # call the function to see it on Airflow UI
    process_user(user_info)
    store_user()
    # clear_users_table()

    # DEFINE DEPENDENCIES
user_processing() # needs to be called to register it with Airflow UI to return a DAG object


    