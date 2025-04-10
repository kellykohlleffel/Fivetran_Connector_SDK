import json
import time
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op

# Configuration for locations
LOCATIONS = {
    "33748_PC": "Cypress, TX, USA",
    "4125_PC": "Brooklyn, NY, USA",
    "26457_PC": "Chicago, IL, USA",
    "39375_PC": "San Francisco, CA, USA"
}

BASE_API_URL = "http://dataservice.accuweather.com/forecasts/v1/daily/5day/"

def create_retry_session():
    """Create requests session with exponential backoff"""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    return [{
        "table": "weather_forecasts",
        "primary_key": ["location_key", "date"],
        "columns": {
            "location_key": "STRING",
            "city": "STRING",
            "date": "NAIVE_DATE",  # Date without time
            "min_temperature": "FLOAT",
            "max_temperature": "FLOAT",
            "day_icon": "INT",
            "day_phrase": "STRING",
            "day_has_precipitation": "BOOLEAN",
            "night_icon": "INT",
            "night_phrase": "STRING",
            "night_has_precipitation": "BOOLEAN",
            "precipitation_probability": "INT",
            "last_modified": "NAIVE_DATETIME"  # Datetime without timezone
        }
    }]

def process_forecast_data(location_key, city, data):
    """Process API response into normalized records"""
    records = []
    current_time = datetime.utcnow().isoformat()

    for day in data.get('DailyForecasts', []):
        try:
            record = {
                "location_key": location_key,
                "city": city,
                "date": day['Date'][:10],
                "min_temperature": day['Temperature']['Minimum']['Value'],
                "max_temperature": day['Temperature']['Maximum']['Value'],
                "day_icon": day['Day']['Icon'],
                "day_phrase": day['Day']['IconPhrase'],
                "day_has_precipitation": day['Day']['HasPrecipitation'],
                "night_icon": day['Night']['Icon'],
                "night_phrase": day['Night']['IconPhrase'],
                "night_has_precipitation": day['Night']['HasPrecipitation'],
                "precipitation_probability": day['Day'].get('PrecipitationProbability', 0),
                "last_modified": current_time
            }
            records.append(record)
        except KeyError as e:
            Logging.warning(f"Missing key {e} in forecast data for {city}")
            continue

    return records

def handle_api_errors(response):
    """Handle common API error scenarios"""
    if response.status_code == 429:
        Logging.warning("Rate limit exceeded - consider reducing sync frequency")
    elif response.status_code == 401:
        raise ValueError("Invalid API key")
    elif 400 <= response.status_code < 500:
        raise ValueError(f"Client error: {response.text}")
    elif response.status_code >= 500:
        raise ValueError(f"Server error: {response.text}")

def update(configuration: dict, state: dict):
    """Main sync function for 5-day forecasts"""
    session = create_retry_session()
    api_key = configuration['api_key']
    request_count = 0

    try:
        for location_key, city in LOCATIONS.items():
            if request_count >= 45:
                Logging.warning("Approaching rate limit - stopping sync")
                break

            url = f"{BASE_API_URL}{location_key}"
            params = {
                'apikey': api_key,
                'details': 'true',
                'metric': 'true'
            }

            try:
                Logging.warning(f"Fetching forecast for {city}")
                response = session.get(url, params=params, timeout=15)
                request_count += 1

                if response.status_code != 200:
                    handle_api_errors(response)
                    continue

                data = response.json()
                records = process_forecast_data(location_key, city, data)

                for record in records:
                    yield op.upsert(
                        table="weather_forecasts",
                        data=record
                    )

                Logging.warning(f"Processed {len(records)} days for {city}")

            except json.JSONDecodeError:
                Logging.warning(f"Invalid JSON response for {city}")
                continue
            except Exception as e:
                Logging.warning(f"Error processing {city}: {str(e)}")
                continue

            # Rate limit buffer
            time.sleep(1)

        # Update sync state
        yield op.checkpoint({
            "last_sync": datetime.utcnow().isoformat(),
            "request_count": request_count
        })

    except Exception as e:
        Logging.warning(f"Critical sync error: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting Accuweather connector debug run...")
    connector.debug()
    Logging.warning("Debug run completed.")