# 🌤️ Daily Weather Monitor – Airflow Pipeline

This project implements an **Apache Airflow** data pipeline that monitors London's weather conditions using the **Open-Meteo API**. The pipeline extracts hourly weather data, summarizes daily statistics, checks for extreme conditions, and loads the results into a **MySQL** database for long-term storage and analysis.

---

## 📁 Project Structure

project-root/
├── config/ # Configuration files (optional, e.g., .env)
├── dags/
│ └── daily_weather_dag.py # 🌀 Main Airflow DAG with the full pipeline
├── logs/ # Airflow task logs
├── docker-compose.yaml # Docker setup for Airflow services
└── .DS_Store # macOS system file (can be ignored)


> ✅ All DAG logic is contained in `dags/daily_weather_dag.py`.

---

## 🚀 Features

- ⏰ **Hourly API requests** to Open-Meteo for current weather in London.
- 📦 **CSV-based staging** of hourly and daily summaries inside the container.
- 📊 **Daily summaries**: min/max temperatures, average wind speed, and rain detection.
- 🚨 **Extreme weather alerts**: checks for high temperatures, rain, snow, and storms.
- 🗃️ **Data persistence**: Loads clean weather data into MySQL tables.
- 🔁 **Runs hourly** with a built-in scheduler (Airflow).
- 🐳 **Docker-compatible** for easy orchestration.

---

## 🛠️ Prerequisites

- Python 3.8+
- Docker + Docker Compose
- Apache Airflow 2.7+ (via Docker or manual)
- MySQL server with a database named `apache_weather_data`
- Python packages:
  - `requests`
  - `pandas`
  - `sqlalchemy`
  - `mysql-connector-python`
  - `python-dotenv`

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


⛓️ DAG Workflow Overview
The DAG defined in daily_weather_dag.py follows this task sequence:

is_api_available
       ↓
extract_transform_weather
       ↓
summarize_weather
       ↓
check_extreme_weather
       ↓
load_to_mysql

6. 
| Task ID                     | Purpose                                               |
| --------------------------- | ----------------------------------------------------- |
| `is_api_available`          | Verifies Open-Meteo API availability                  |
| `extract_transform_weather` | Fetches current data and appends it to hourly CSV     |
| `summarize_weather`         | Creates daily metrics (min/max temp, avg wind, rain)  |
| `check_extreme_weather`     | Detects rain, snow, high temps, and storms from codes |
| `load_to_mysql`             | Loads hourly and summary data into MySQL database     |


🗃️ MySQL Database Schema
You need to create a MySQL database named apache_weather_data with two tables:

weather_hourly
date, time, temperature, relative_humidity, wind_speed, rain, precipitation, weather_code, alerts
weather_summary
date, min_temp, max_temp, avg_wind_speed, rain_expected
Note: Tables are created automatically if they don't exist (via SQLAlchemy).

📌 New in This Version
✅ Added 3 DAG tasks:
summarize_weather
check_extreme_weather
load_to_mysql
✅ Complete inline alerting logic
✅ Local file persistence in /tmp
✅ Enhanced logging and error handling

🔮 Future Improvements
Email/Slack alerts for extreme weather
Configurable cities (via Airflow Variables)
Dashboard integration (Streamlit, Metabase, etc.)
CSV cleanup and rotation policy
Historical trend analysis
Support for Parquet files or cloud storage

