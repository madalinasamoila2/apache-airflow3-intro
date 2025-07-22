from airflow import DAG # used to import the DAG class from Apache Airflow
# DAG is a collection of all the tasks you want to run and every airflow workflow must be wrapped in a DAG object to define the schedule, start date, .. 
from airflow.providers.standard.operators.python import PythonOperator # let's you run a python function as a task (data extraction, transformation)
from datetime import datetime, timedelta # datetime is used to define the DAG's start date, timedelta used to define DAG's schedule
import requests # retrieve data from API
import pandas as pd
import os
import mysql.connector # connect to mysql
from sqlalchemy import create_engine
from dotenv import load_dotenv
import csv

 
# Task: is_api_available to check whether the API link is available ------------------------------------------------------------------
def is_api_available():
    url = "https://api.open-meteo.com/v1/forecast?latitude=51.5072&longitude=0.1276&current=temperature_2m"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        print(" API is available.")
    except Exception as e:
        raise RuntimeError(f"API not reachable: {e}")


# Task: extract_transform_weather which extracts & transforms weather data from the API, extracted hourly to be stored as csv/mysql -----
def extract_transform_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=51.5072&longitude=0.1276&current=temperature_2m,relative_humidity_2m,wind_speed_10m,rain,precipitation,weather_code&timezone=Europe%2FLondon"
    response = requests.get(url)
    data = response.json()

    # Create dataframe with necessary fields
    from datetime import datetime
    import pandas as pd 
    summary = {
        "date":datetime.now().strftime("%Y-%m-%d"),
        "time": data["current"]["time"],
        "temperature":data["current"]["temperature_2m"],
        "relative_humidity": data["current"]["relative_humidity_2m"],
        "wind_speed": data["current"]["wind_speed_10m"],
        "rain": data["current"]["rain"],
        "precipitation": data["current"]["precipitation"],
        "weather_code": data["current"]["weather_code"]
    }

    # Add alert message as a new column to determine the weather condition for that hour
    code = int(summary["weather_code"])
    temp = summary["temperature"]
    alert_messages = []

    # Weather codes taken from the open-meteo link
    rain_codes = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}
    snow_codes = {71, 73, 75, 77, 85, 86}
    thunderstorm_codes = {95, 96, 99}

    if temp > 35:
        alert_messages.append("🔥 High temperature alert!")
    if code in rain_codes:
        alert_messages.append("🌧️ Rain expected.")
    if code in snow_codes:
        alert_messages.append("❄️ Snow expected.")
    if code in thunderstorm_codes:
        alert_messages.append("⛈️ Thunderstorm alert!")

    # Combine alerts into a single string
    summary["alerts"] = " | ".join(alert_messages) if alert_messages else "No alerts"

    df = pd.DataFrame([summary])

    # Save to local file inside Airflow container 
    import os
    filename = f"/tmp/hourly_summary_{summary['date']}.csv" # temporarily stores the data in a temporary folder in docker
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', index=False, header=False)  # append without header
        print(f"Saving appended CSV file to: {filename}")
    else:
        df.to_csv(filename, index=False, header=True)  # write with header
        print(f"Saving CSV file to: {filename}")

    # Return small snippet for XCom
    return df.to_dict()


# Task: summarise_daily_weather used as a DAG task to aggregate values and stored them in a daily summary csv
def summarize_daily_weather():
    today = (datetime.now()).strftime("%Y-%m-%d")
    hourly_file = f"/tmp/hourly_summary_{today}.csv"
    print(f"Looking for hourly file: {hourly_file}") # printing steps to help me check if the data is being retrieved
    
    if not os.path.exists(hourly_file):
        raise FileNotFoundError(f"No hourly data found for {today}")

    df = pd.read_csv(hourly_file)
    print(f"Read {len(df)} rows from {hourly_file}")

    if df.empty:
        raise ValueError("Hourly CSV is empty")
    
    summary_file = f"/tmp/daily_summary_{today.replace('-', '_')}.csv"
    print(f"Saving daily summary to {summary_file}")

    summary = {
        "date": today,
        "min_temp": df["temperature"].min(),
        "max_temp": df["temperature"].max(),
        "avg_wind_speed": round(df["wind_speed"].mean(), 2),
        "rain_expected": df["rain"].sum() > 0
    }

    df_summary = pd.DataFrame([summary])
    df_summary.to_csv(summary_file, index=False)
    
    # Return small snippet for XCom
    if df_summary is None:
        raise ValueError("df_summary is None")

    return summary

# Task: check_extreme_weather used as a DAG task (mainky on Docker)to determine weather conditions hourly and take precautions
def check_extreme_weather():
    import pandas as pd
    import os
    from datetime import datetime

    today = datetime.now().strftime("%Y-%m-%d")
    hourly_file = f"/tmp/hourly_summary_{today}.csv"
    summary_file = f"/tmp/daily_summary_{today.replace('-', '_')}.csv"

    if not os.path.exists(summary_file):
        raise FileNotFoundError(f"No daily summary found for {today}")
    if not os.path.exists(hourly_file):
        raise FileNotFoundError(f"No hourly summary found for {today}")

    hourly_df = pd.read_csv(hourly_file)
    summary_df = pd.read_csv(summary_file)

    if summary_df.empty or hourly_df.empty:
        raise ValueError("Summary or hourly file is empty")

    max_temp = summary_df.loc[0, "max_temp"]

    # Define code sets
    rain_codes = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}
    snow_codes = {71, 73, 75, 77, 85, 86}
    thunderstorm_codes = {95, 96, 99}

    weather_codes_today = set(hourly_df["weather_code"].dropna().astype(int))

    alert_messages = []

    # High temp alert
    if max_temp > 35:
        alert_messages.append(f"🔥 High temperature alert! Max temp is {max_temp}°C.")
    # Rain alert
    if weather_codes_today & rain_codes:
        alert_messages.append(f"🌧️ Rain expected today. Weather code is {weather_codes_today}. Take precautions.")
    # Snow alert
    if weather_codes_today & snow_codes:
        alert_messages.append(f"❄️ Snow expected today. Weather code is {weather_codes_today}. Schedule maintenance.")
    # Thunderstorm alert
    if weather_codes_today & thunderstorm_codes:
        alert_messages.append(f"⛈️ Thunderstorm alert! Weather code is {weather_codes_today}. Stay safe and prepare response teams.")

    if alert_messages:
        print("\n".join(alert_messages))
    else:
        print("✅ No extreme weather conditions detected today.")

    return alert_messages


# Task: load_to_mysql used to load the created hourly_summary & daily_summary and store into MySQL Database for future use.
def load_to_mysql(**kwargs):
    try: 
        today = datetime.now().strftime("%Y-%m-%d")
        hourly_file = f"/tmp/hourly_summary_{today}.csv"
        summary_file = f"/tmp/daily_summary_{today.replace('-', '_')}.csv"

        if not os.path.exists(hourly_file):
            raise FileNotFoundError("Hourly file not found.")
        if not os.path.exists(summary_file):
            raise FileNotFoundError("Summary daily file not found.")

        df_hourly = pd.read_csv(hourly_file)
        df_summary = pd.read_csv(summary_file)

    # Database credentials 
        db_host = "host.docker.internal" # originally is localhost but because we are using Docker, host.docker.internal allow your Docker container (Airflow) to connect to your host machine's MySQL server
        db_user = "root"
        db_password = ''
        db_database = "apache_weather_data"

    # Create a connection to the database
        conn = mysql.connector.connect(
            host=db_host, user=db_user, password=db_password, database=db_database
        )

        # Create a SQLAlchemy engine to connect to MySQL database
        engine = create_engine(
            f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_database}"
        )

    # Insert DataFrame into a SQL table, with a name of table_name given
        df_hourly.to_sql(name="weather_hourly", con=engine, if_exists="replace", index=False)
        df_summary.to_sql(name="weather_summary", con=engine, if_exists="replace", index=False)
       
    # Close the database connection
        conn.close()

    except Exception as error:
        print(f"An error occurred: {error}")

default_args = {
    'owner': 'airflow',
    'retries': 0,
}


# Airflow DAG 'simple_weather_to_csv' which runs the tasks created above using a schedule, start date -------------------------------------
# # DAG is a collection of all the tasks you want to run and every airflow workflow must be wrapped in a DAG object to define the schedule, start date, .. 
with DAG(
    dag_id="simple_weather_to_csv", # unique identifier for the DAG
    default_args=default_args,     # applies default parameters
    start_date=datetime(2025, 7, 1), # start date
    schedule="@hourly",       # airflow starts scheduling every hour
    catchup=False           # prevents airflow from running past (missed) runs when you first execute the tasks
) as hourly_dag:

# Create a task with Python Operator
    task_check_api = PythonOperator(
        task_id="is_api_available",
        python_callable=is_api_available
    )
    task_extract_transform = PythonOperator( 
        task_id="extract_transform_weather", 
        python_callable=extract_transform_weather 
    )
    task_summarize = PythonOperator(
        task_id="summarize_weather",
        python_callable=summarize_daily_weather
    )
    task_check_extreme_weather = PythonOperator(
        task_id="check_extreme_weather",
        python_callable=check_extreme_weather
    )
    task_load_to_mysql = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql
    )
    # Order in which to run the tasks
    task_check_api >> task_extract_transform >> task_summarize >> task_check_extreme_weather >> task_load_to_mysql





