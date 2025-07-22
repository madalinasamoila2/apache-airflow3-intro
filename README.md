# Daily Weather Monitor - Airflow Pipeline

This project implements an **Apache Airflow** data pipeline that monitors London's weather conditions using the **Open-Meteo API**. The pipeline extracts hourly weather data, summarizes daily statistics, checks for extreme conditions, and loads the results into a **MySQL** database for long-term storage and analysis.

<img width="983" height="118" alt="Screenshot 2025-07-22 at 12 08 20" src="https://github.com/user-attachments/assets/526313c1-d91e-4688-b7f7-73c6d8d37650" />

---
## Project Structure
This project is designed to run on your local machine using Docker. Airflow, MySQL, and supporting services are containerised via docker-compose.yaml, and require your host machine to be accessible via host.docker.internal.

You’ll need: Docker & Docker Compose installed A MySQL instance running locally (or use the one defined in your docker-compose.yaml) Airflow will connect to your local MySQL using host.docker.internal.

<img width="703" height="503" alt="Screenshot 2025-07-22 at 13 00 36" src="https://github.com/user-attachments/assets/388455af-efc8-4934-8652-bacb2dea0386" />

>  All DAG logic is contained in `dags/daily_weather_dag.py`.

---

## DAG Tasks 

- **is_api_availables** to check if the API link works and is available
- **extract_transform_weather** which extracts weather data hourly from the API, transforms it using necessary columns and stores the data in a temporary folder in docker
- **summarise_daily_weather**:  summarizes daily min/max temperatures, average wind speed, and rain detection and stores the data in a temporary folder in docker
- **check_extreme_weather**: checks for high temperatures, rain, snow, and storms and sends an alert message to docker to take precautions 
- **load_to_mysql**: Loads clean weather data (hourly_summary/daily_summary) into MySQL tables and store into database for future use.
- **DAG: simple_weather_to_csv** runs the DAG tasks alltogether using with a built-in scheduler (Airflow) using things like start date, schedule

---

## 🛠️ Tools & Technologies
Apache Airflow (workflow orchestration)
Open-Meteo API (data source)
Python + Pandas (ETL logic)
MySQL (data storage)
Docker & Docker Compose (local dev environment)

- Apache Airflow (workflow orchestration)
- Open-Meteo API (data source)
- Python + Pandas (ETL logic)
- MySQL (data storage)
- Docker & Docker Compose (local dev environment)

---

## ⚙️ Setup Instructions

1. **Clone the repository**

   ```bash
   git clone https://github.com/your-username/weather-airflow-pipeline.git
   cd weather-airflow-pipeline

2. (Optional) Install Python dependencies locally
pip install -r requirements.txt

3. Start Airflow using Docker Compose
docker-compose up airflow-init   # Initialize metadata DB
docker-compose up                # Launch webserver, scheduler, etc.

4. Access the Airflow web UI
Open http://localhost:8080
Default credentials: airflow / airflow

5. Trigger the DAG
DAG ID: simple_weather_to_csv
Either wait for hourly scheduling or trigger manually from the UI.


## ⛓️ DAG Workflow Overview
The DAG defined in daily_weather_dag.py follows this task sequence:

<img width="1212" height="164" alt="image" src="https://github.com/user-attachments/assets/9e7c3a5d-f568-475d-a420-8c6c8f2d8a1b" />


6. 
| Task ID                     | Purpose                                               |
| --------------------------- | ----------------------------------------------------- |
| `is_api_available`          | Verifies Open-Meteo API availability                  |
| `extract_transform_weather` | Fetches current data and appends it to hourly CSV     |
| `summarize_weather`         | Creates daily metrics (min/max temp, avg wind, rain)  |
| `check_extreme_weather`     | Detects rain, snow, high temps, and storms from codes |
| `load_to_mysql`             | Loads hourly and summary data into MySQL database     |


##  MySQL Database Schema
You need to create a MySQL database named apache_weather_data with two tables:

weather_hourly
date, time, temperature, relative_humidity, wind_speed, rain, precipitation, weather_code, alerts

weather_summary
date, min_temp, max_temp, avg_wind_speed, rain_expected
Note: Tables are created automatically if they don't exist (via SQLAlchemy).


## Future Improvements
- Email/Slack alerts for extreme weather
- Configurable cities (via Airflow Variables)
- Dashboard integration (Streamlit, etc.)
- CSV cleanup 
- Historical trend analysis


