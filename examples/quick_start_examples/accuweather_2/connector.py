import json
import time
from datetime import datetime, timedelta
import requests as rq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op


def create_retry_session():
    """Create a requests session with retry logic for handling rate limits"""
    session = rq.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def get_api_key(configuration):
    """Retrieve the API key from the configuration."""
    api_key = configuration.get('api_key')
    if not api_key:
        raise KeyError("Missing api_key in configuration")
    return str(api_key)


def schema(configuration: dict):
    """Define the table for Fivetran."""
    return [
        {
            "table": "daily_forecasts",
            "primary_key": ["id"],
            "deletion_detection_enabled": True,  # Enable Fivetran's built-in deletion detection
            "columns": {
                "id": "STRING",
                "location_key": "STRING",
                "location_name": "STRING",
                "forecast_date": "STRING",
                "min_temp_f": "FLOAT",
                "max_temp_f": "FLOAT",
                "day_icon": "INT",
                "day_icon_phrase": "STRING",
                "day_precipitation_probability": "INT",
                "day_rain_probability": "INT",
                "day_snow_probability": "INT",
                "day_ice_probability": "INT",
                "day_wind_speed_mph": "FLOAT",
                "day_wind_direction": "STRING",
                "night_icon": "INT",
                "night_icon_phrase": "STRING",
                "night_precipitation_probability": "INT",
                "night_rain_probability": "INT",
                "night_snow_probability": "INT",
                "night_ice_probability": "INT",
                "night_wind_speed_mph": "FLOAT",
                "night_wind_direction": "STRING",
                "mobile_link": "STRING",
                "link": "STRING",
                "last_modified": "STRING"
            }
        }
    ]


def get_locations():
    """Return a list of location dictionaries with keys and names.
    
    Note: The connector is configured to use the 5-day forecast endpoint 
    which is available under the free tier AccuWeather API.
    
    Free tier allows access to:
    - Current conditions
    - 24 hours historical current conditions
    - Daily forecast (5 days)
    - Hourly forecast (12 hours)
    - Indices (5 days)
    """
    return [
        {"key": "33748_PC", "name": "Cypress, TX"},
        {"key": "4125_PC", "name": "Brooklyn, NY"},
        {"key": "26457_PC", "name": "Chicago, IL"},
        {"key": "39375_PC", "name": "San Francisco, CA"}
    ]


def make_api_request(session, api_key, endpoint, params=None):
    """Make API request with error handling and rate limit management"""
    base_url = "https://dataservice.accuweather.com"

    # Initialize params if None
    if params is None:
        params = {}
    
    # AccuWeather expects the API key as a query parameter
    # Create a copy of params to avoid modifying the original
    request_params = params.copy()
    request_params['apikey'] = api_key

    try:
        # Construct URL exactly matching the working format
        url = f"{base_url}/{endpoint}"
        
        # Only log the endpoint without the API key
        Logging.warning(f"Making request to {url}")
        
        # Add delay to respect rate limits
        time.sleep(0.5)
        
        # Create a fresh session for each request to avoid any potential session issues
        fresh_session = rq.Session()
        response = fresh_session.get(url, params=request_params, timeout=30, verify=True)
        
        # Check for rate limit errors
        if response.status_code == 429:
            Logging.warning("Rate limit reached. Implementing backoff...")
            time.sleep(60)  # Wait 1 minute before retrying
            response = fresh_session.get(url, params=request_params, timeout=30)
        
        response.raise_for_status()  # Raise an error for 4xx/5xx responses
        
        # Log the redacted URL without showing the API key
        redacted_url = response.url.split('apikey=')[0] + 'apikey=****'
        Logging.warning(f"Successful request to: {redacted_url}")
        
        data = response.json()
        Logging.warning(f"Successfully retrieved data from {endpoint}")
        
        # Log a sample of the response structure to help with debugging
        if 'Headline' in data:
            Logging.warning(f"Headline text: {data['Headline'].get('Text', 'N/A')}")
        if 'DailyForecasts' in data and len(data['DailyForecasts']) > 0:
            Logging.warning(f"First forecast date: {data['DailyForecasts'][0].get('Date', 'N/A')}")
        
        return data
    except rq.exceptions.HTTPError as e:
        # Redact API key from error logs
        error_text = e.response.text
        if api_key in error_text:
            error_text = error_text.replace(api_key, "****")
        
        Logging.warning(f"HTTP Error: {e.response.status_code} - {error_text}")
        
        # Redact API key from URL in error logs
        if hasattr(e.response, 'url'):
            redacted_url = e.response.url.split('apikey=')[0] + 'apikey=****'
            Logging.warning(f"Failed URL: {redacted_url}")
        
        if e.response.status_code == 401:
            Logging.warning("Authentication failed. Please check your API key and ensure it's still valid.")
        raise
    except Exception as e:
        # Ensure API key is not logged in the error message
        error_str = str(e)
        if api_key in error_str:
            error_str = error_str.replace(api_key, "****")
        Logging.warning(f"Unexpected error: {error_str}")
        raise


def process_forecast_data(data, location_key, location_name):
    """Process forecast data from the Accuweather API response."""
    processed_records = []
    
    # Check for empty or invalid data
    if not data or 'DailyForecasts' not in data:
        Logging.warning(f"No forecast data available for location {location_key}")
        return processed_records
    
    for forecast in data.get('DailyForecasts', []):
        try:
            # Extract forecast date
            date_str = forecast.get('Date')
            if not date_str:
                Logging.warning(f"Skipping forecast with missing date for location {location_key}")
                continue
                
            try:
                forecast_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                Logging.warning(f"Invalid date format: {date_str}")
                continue
                
            # Extract temperature data
            temperature = forecast.get('Temperature', {})
            min_temp = temperature.get('Minimum', {}).get('Value')
            max_temp = temperature.get('Maximum', {}).get('Value')
            
            # Default values if data is missing
            if min_temp is None:
                min_temp = 0
                Logging.warning(f"Missing minimum temperature for {location_name} on {forecast_date}")
            
            if max_temp is None:
                max_temp = 0
                Logging.warning(f"Missing maximum temperature for {location_name} on {forecast_date}")
            
            # Extract day forecast data
            day = forecast.get('Day', {})
            
            # Extract night forecast data
            night = forecast.get('Night', {})
            
            # Create unique ID
            unique_id = f"{location_key}_{forecast_date}"
            
            # Process day precipitation data with safer extraction
            day_precip_prob = day.get('PrecipitationProbability')
            if day_precip_prob is None:
                day_precip_prob = 0
            
            day_rain_prob = day.get('RainProbability')
            if day_rain_prob is None:
                day_rain_prob = 0
                
            day_snow_prob = day.get('SnowProbability')
            if day_snow_prob is None:
                day_snow_prob = 0
                
            day_ice_prob = day.get('IceProbability')
            if day_ice_prob is None:
                day_ice_prob = 0
            
            # Process night precipitation data with safer extraction
            night_precip_prob = night.get('PrecipitationProbability')
            if night_precip_prob is None:
                night_precip_prob = 0
                
            night_rain_prob = night.get('RainProbability')
            if night_rain_prob is None:
                night_rain_prob = 0
                
            night_snow_prob = night.get('SnowProbability')
            if night_snow_prob is None:
                night_snow_prob = 0
                
            night_ice_prob = night.get('IceProbability')
            if night_ice_prob is None:
                night_ice_prob = 0
            
            # Process day wind data
            day_wind_speed = 0
            day_wind_direction = ""
            
            if 'Wind' in day:
                day_wind = day.get('Wind', {})
                day_wind_speed = day_wind.get('Speed', {}).get('Value', 0)
                day_wind_direction = day_wind.get('Direction', {}).get('English', "")
            
            # Process night wind data
            night_wind_speed = 0
            night_wind_direction = ""
            
            if 'Wind' in night:
                night_wind = night.get('Wind', {})
                night_wind_speed = night_wind.get('Speed', {}).get('Value', 0)
                night_wind_direction = night_wind.get('Direction', {}).get('English', "")
            
            processed_record = {
                'id': unique_id,
                'location_key': location_key,
                'location_name': location_name,
                'forecast_date': forecast_date,
                'min_temp_f': float(min_temp),
                'max_temp_f': float(max_temp),
                'day_icon': int(day.get('Icon', 0)),
                'day_icon_phrase': str(day.get('IconPhrase', '')),
                'day_precipitation_probability': int(day_precip_prob),
                'day_rain_probability': int(day_rain_prob),
                'day_snow_probability': int(day_snow_prob),
                'day_ice_probability': int(day_ice_prob),
                'day_wind_speed_mph': float(day_wind_speed),
                'day_wind_direction': str(day_wind_direction),
                'night_icon': int(night.get('Icon', 0)),
                'night_icon_phrase': str(night.get('IconPhrase', '')),
                'night_precipitation_probability': int(night_precip_prob),
                'night_rain_probability': int(night_rain_prob),
                'night_snow_probability': int(night_snow_prob),
                'night_ice_probability': int(night_ice_prob),
                'night_wind_speed_mph': float(night_wind_speed),
                'night_wind_direction': str(night_wind_direction),
                'mobile_link': str(forecast.get('MobileLink', '')),
                'link': str(forecast.get('Link', '')),
                'last_modified': datetime.utcnow().isoformat()
            }
            
            processed_records.append(processed_record)
            
        except Exception as e:
            Logging.warning(f"Error processing forecast record: {str(e)}")
            continue
    
    Logging.warning(f"Successfully processed {len(processed_records)} forecast records for {location_name}")
    return processed_records


def update(configuration: dict, state: dict):
    """Retrieve the most recent forecast data from the Accuweather API.
    
    Important: This function always returns the complete current dataset for each sync.
    Fivetran will automatically detect deleted records by comparing primary keys 
    between syncs.
    """
    try:
        api_key = get_api_key(configuration)
        if not api_key:
            Logging.warning("Missing or invalid API key. Please check your configuration.")
            yield op.checkpoint(state)
            return
            
        locations = get_locations()
        
        # Get the last sync date from state or use default (3 days ago)
        # Note: We still track this for logging/debugging purposes
        last_sync = state.get('last_sync_date')
        if last_sync:
            last_sync_date = datetime.strptime(last_sync, '%Y-%m-%d %H:%M:%S')
        else:
            last_sync_date = datetime.utcnow() - timedelta(days=3)
            
        Logging.warning(f"Starting sync for forecasts since {last_sync_date}")
        
        total_records = 0
        error_count = 0
        
        # Debug output of API key usage (fully masked for security)
        Logging.warning(f"Using API key (masked for security)")
        
        # Track all current forecast IDs for logging purposes
        current_forecast_ids = set()
        
        # Process each location
        for location in locations:
            location_key = location["key"]
            location_name = location["name"]
            
            Logging.warning(f"Fetching forecast data for {location_name} (Key: {location_key})")
            
            try:
                # Use the 5-day forecast endpoint which is accessible with the free tier API key
                endpoint = f"forecasts/v1/daily/5day/{location_key}"
                params = {
                    "language": "en-us",
                    "details": "true",  # Get detailed information
                    "metric": "false"
                }
                
                forecast_data = make_api_request(None, api_key, endpoint, params)
                
                # Check if we received valid data
                if not forecast_data or 'DailyForecasts' not in forecast_data:
                    Logging.warning(f"Invalid or empty response for {location_name}")
                    error_count += 1
                    continue
                
                # Log the number of forecasts received
                forecast_count = len(forecast_data.get('DailyForecasts', []))
                Logging.warning(f"Received {forecast_count} days of forecast data for {location_name}")
                
                # Process forecast data and yield upsert operations
                processed_records = process_forecast_data(forecast_data, location_key, location_name)
                
                # Track all current forecast IDs for logging
                for record in processed_records:
                    current_forecast_ids.add(record['id'])
                    
                    # Since we're always getting a complete dataset for each location,
                    # we can simply upsert all records and let Fivetran handle deletion detection
                    yield op.upsert(
                        table="daily_forecasts",
                        data=record
                    )
                
                total_records += len(processed_records)
                
                Logging.warning(f"Processed {len(processed_records)} records for {location_name}")
                
            except Exception as e:
                # Ensure API key is not in the error message
                error_str = str(e)
                if api_key in error_str:
                    error_str = error_str.replace(api_key, "****")
                Logging.warning(f"Error processing forecasts for {location_name}: {error_str}")
                error_count += 1
                # Continue with other locations even if one fails
                continue
        
        if error_count == len(locations):
            Logging.warning("All location requests failed. Please check your API key and location IDs.")
        
        # Log the number of current forecast IDs for tracking purposes
        Logging.warning(f"Current sync contains {len(current_forecast_ids)} unique forecast IDs")
        
        # Checkpoint after sync attempt (even if there were errors)
        # We only need to store the sync timestamp, not any IDs
        yield op.checkpoint({
            "last_sync_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        Logging.warning(f"Sync complete - processed {total_records} total forecast records")
        
    except Exception as e:
        # Ensure API key is not in the error message
        error_str = str(e)
        if api_key in error_str:
            error_str = error_str.replace(api_key, "****")
        Logging.warning(f"Error during sync process: {error_str}")
        # Still checkpoint to avoid repeated failures
        if state:
            yield op.checkpoint(state)
        raise


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting Accuweather Forecast connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")