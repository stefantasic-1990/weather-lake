import requests
import hashlib
from datetime import datetime

temp_file_dir="/opt/airflow/temp/"

def download_weather_data_handler():
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
    temp_file_name = f"openmeteo_16day_hourly_forecast_{timestamp}.csv"
    temp_file_path = temp_file_dir+temp_file_name
    openmeteo_endpoint = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=43.7064&longitude=-79.3986&"
        "hourly=temperature_2m,relative_humidity_2m,precipitation,"
        "wind_speed_10m,wind_direction_10m,pressure_msl&forecast_days=16&format=csv"
    )
    hasher=hashlib.sha256()

    with requests.get(openmeteo_endpoint, stream=True, timeout=60) as req:
        req.raise_for_status()

        with open(temp_file_path, "wb") as temp_file:
            for chunk in req.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                hasher.update(chunk)
                temp_file.write(chunk)

    temp_file_digest = hasher.hexdigest()
    
    return str(temp_file_path), temp_file_digest