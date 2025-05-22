import json  # Import the json module to handle JSON data.
from datetime import datetime  # Import datetime for handling date and time conversions.

import requests as rq  # Import the requests module for making HTTP requests, aliased as rq.
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

def schema(configuration: dict):
    return [
        {
            "table": "forecast",  # Name of the table in the destination.
            "primary_key": ["startTime", "zip_code"],  # Primary key column(s) for the table.
        },
        {
            "table": "zip_code",  # Name of the table for zip code metadata
            "primary_key": ["zip_code"],  # Primary key column for the table.
        }
    ]

def get_coordinates_from_zip(zip_code: str) -> tuple:
    """Get latitude and longitude for a zip code using Zippopotam.us API."""
    url = f"https://api.zippopotam.us/us/{zip_code}"
    log.info(f"Requesting coordinates for ZIP code {zip_code}")
    response = rq.get(url)
    response.raise_for_status()
    data = response.json()
    
    log.fine(f"API Response: {json.dumps(data, indent=2)}")
    
    # Extract coordinates from the response
    zip_info = data['places'][0]  # Get the first place in the zip code
    lat = float(zip_info['latitude'])
    lon = float(zip_info['longitude'])
    zip_info["zip_code"] = zip_code
    
    log.info(f"Found coordinates: ({lat}, {lon})")
    return (lat, lon), zip_info  # Return both coordinates and metadata

def get_forecast_url(lat: float, lon: float) -> str:
    """Get the forecast URL for a location using the NWS API's two-step process."""
    headers = {
        "User-Agent": "Fivetran Weather Connector (contact: your-email@example.com)"
    }
    
    # Step 1: Get the metadata for the location
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    response = rq.get(points_url, headers=headers)
    response.raise_for_status()
    points_data = response.json()
    
    # Step 2: Get the forecast URL from the metadata
    forecast_url = points_data["properties"]["forecast"]
    return forecast_url

def update(configuration: dict, state: dict):
    log.warning("Example: api.weather.gov")

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['startTime'] if 'startTime' in state else '0001-01-01T00:00:00Z'

    # Read zip codes from configuration
    zip_codes_str = configuration.get('zip_codes', '94612')  # Default to Oakland
    zip_codes = [zip_code.strip() for zip_code in zip_codes_str.split(',')]
    
    for zip_code in zip_codes:
        try:
            # Get coordinates and metadata for the zip code
            (lat, lon), metadata = get_coordinates_from_zip(zip_code)
            
            # Store the zip code metadata
            yield op.upsert(table="zip_code", data=metadata)
            
            # Get the forecast URL using the NWS API's two-step process
            forecast_url = get_forecast_url(lat, lon)
            log.info(f"Got forecast URL for {zip_code}: {forecast_url}")
            
            # Get the forecast data
            headers = {
                "User-Agent": "Fivetran Weather Connector (contact: cherillin.abeel@fivetran.com)"
            }
            response = rq.get(forecast_url, headers=headers)
            response.raise_for_status()

            # Parse the JSON response to get the forecast periods of the weather forecast.
            data = response.json()
            forecast_periods = data['properties']['periods']

            # This message will show both during debugging and in production.
            log.info(f"number of forecast_periods={len(forecast_periods)}")

            for forecast in forecast_periods:
                # Skip data points we already synced by comparing their start time with the cursor.
                if str2dt(forecast['startTime']) < str2dt(cursor):
                    continue

                # Add zip code to the period data
                forecast['zip_code'] = zip_code
                # This log message will only show while debugging.
                log.fine(f"forecast_period={forecast['name']} for zip code {zip_code}")
                
                # Yield an upsert operation to insert/update the row in the "forecast" table.
                yield op.upsert(table="forecast", data=forecast)

        except Exception as e:
            raise e

    # Update the cursor to the end time of the current period.
    cursor = forecast['endTime']
    yield op.checkpoint(state={"startTime": cursor})


# Define a helper function to convert a string to a datetime object.
def str2dt(incoming: str) -> datetime:
    return datetime.strptime(incoming, "%Y-%m-%dT%H:%M:%S%z")

# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Try loading the configuration from the file
        with open("configuration.json", 'r') as f:
            configuration = json.load(f)
    except FileNotFoundError:
        # Fallback to an empty configuration if the file is not found
        configuration = {}
    # Allows testing the connector directly
    connector.debug(configuration=configuration)
