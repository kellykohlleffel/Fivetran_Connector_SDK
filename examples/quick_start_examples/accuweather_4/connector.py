from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, List, Any
import requests
from datetime import datetime
import time

LOCATIONS = [
    {"key": "33748_PC", "name": "Cypress, TX"},
    {"key": "4125_PC", "name": "Brooklyn, NY"},
    {"key": "26457_PC", "name": "Chicago, IL"},
    {"key": "39375_PC", "name": "San Francisco, CA"}
]

BASE_URL = "https://dataservice.accuweather.com/forecasts/v1/daily/5day"

def validate_api_key(api_key: str) -> bool:
    """Validate the API key by making a test request"""
    test_url = f"{BASE_URL}/{LOCATIONS[0]['key']}?apikey={api_key}&metric=true"
    try:
        response = requests.get(test_url)
        return response.status_code == 200
    except:
        return False

def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the table schema for the AccuWeather 5-day forecast connector
    """
    return [{
        "table": "daily_forecasts",
        "primary_key": ["location_key", "date"],
        "columns": {
            "location_key": "STRING",
            "location_name": "STRING",
            "date": "UTC_DATETIME",
            "min_temp": "FLOAT",
            "max_temp": "FLOAT",
            "day_phrase": "STRING",
            "night_phrase": "STRING",
            "day_precipitation_probability": "INT",
            "night_precipitation_probability": "INT"
        }
    }]

def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch and process 5-day forecast data for specified locations
    """
    api_key = configuration.get("api_key")

    # Validate API key first
    if not validate_api_key(api_key):
        log.severe("Invalid or expired API key. Please check your AccuWeather API key.")
        return

    request_count = state.get("request_count", 0)
    last_sync = state.get("last_sync", "")

    # Check if we've exceeded the daily API limit
    if request_count >= 50:
        log.severe(f"Daily API limit reached: {request_count} requests made")
        return

    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    records_processed = 0

    try:
        for location in LOCATIONS:
            # Increment request counter
            request_count += 1

            # Make API request with exponential backoff retry
            max_retries = 3
            retry_delay = 1

            for attempt in range(max_retries):
                try:
                    url = f"{BASE_URL}/{location['key']}?apikey={api_key}&metric=true"
                    log.info(f"Fetching forecast data for {location['name']}")

                    response = requests.get(url)
                    response.raise_for_status()
                    forecast_data = response.json()

                    # Process daily forecasts
                    for daily in forecast_data.get("DailyForecasts", []):
                        forecast_date = datetime.strptime(daily["Date"], "%Y-%m-%dT%H:%M:%S%z")
                        forecast_date_str = forecast_date.strftime('%Y-%m-%dT%H:%M:%SZ')

                        yield op.upsert(
                            "daily_forecasts",
                            {
                                "location_key": location["key"],
                                "location_name": location["name"],
                                "date": forecast_date_str,
                                "min_temp": daily["Temperature"]["Minimum"]["Value"],
                                "max_temp": daily["Temperature"]["Maximum"]["Value"],
                                "day_phrase": daily["Day"]["IconPhrase"],
                                "night_phrase": daily["Night"]["IconPhrase"],
                                "day_precipitation_probability": daily["Day"]["PrecipitationProbability"],
                                "night_precipitation_probability": daily["Night"]["PrecipitationProbability"]
                            }
                        )
                        records_processed += 1

                    # Successful request, break retry loop
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        log.severe(f"Error fetching data for {location['name']}: {str(e)}")
                    else:
                        log.warning(f"Retry {attempt + 1} for {location['name']}: {str(e)}")
                        time.sleep(retry_delay)
                        retry_delay *= 2

            # Checkpoint after each location
            if records_processed > 0:
                yield op.checkpoint({
                    "request_count": request_count,
                    "last_sync": current_time
                })

    except Exception as e:
        log.severe(f"Unexpected error: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()