import json
import os
import time
from datetime import datetime
import requests as rq
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op

def create_retry_session():
    """Create a requests session with retry logic"""
    session = rq.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_api_key(configuration):
    """Retrieve the API key from the configuration."""
    try:
        api_key = configuration.get('api_key')
        if not api_key:
            raise KeyError("No API key found in configuration")
        return str(api_key)
    except Exception as e:
        raise KeyError(f"Error retrieving API key: {str(e)}")

def schema(configuration: dict):
    """Define the table schemas for Fivetran."""
    return [
        {
            "table": "thingstodo",
            "primary_key": ["activity_id"],
            "columns": {
                "activity_id": "STRING",
                "park_id": "STRING",
                "park_name": "STRING",
                "park_state": "STRING",
                "title": "STRING",
                "short_description": "STRING",
                "accessibility_information": "STRING",
                "location": "STRING",
                "url": "STRING",
                "duration": "STRING",
                "tags": "STRING"
            },
        },
        {
            "table": "parks",
            "primary_key": ["park_id"],
            "columns": {
                "park_id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "state": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
                "activities": "STRING",
                "designation": "STRING"
            },
        },
        {
            "table": "feespasses",
            "primary_key": ["pass_id"],
            "columns": {
                "pass_id": "STRING",
                "park_id": "STRING",
                "park_name": "STRING",
                "title": "STRING",
                "cost": "FLOAT",
                "description": "STRING",
                "valid_for": "STRING",
            },
        }
    ]

def make_api_request(session, endpoint, params):
    """Make API request with error handling and logging"""
    try:
        # Create a copy of params with masked API key for logging
        log_params = params.copy()
        if 'api_key' in log_params:
            log_params['api_key'] = '***'
        
        Logging.warning(f"Making request to {endpoint} with params: {log_params}")
        response = session.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        Logging.warning(f"Response total count: {len(data.get('data', []))}")
        if len(data.get('data', [])) > 0:
            Logging.warning("Sample of first response item:")
            first_item = data['data'][0]
            Logging.warning(f"Name: {first_item.get('fullName')}")
            Logging.warning(f"Designation: {first_item.get('designation')}")
            Logging.warning(f"Park Code: {first_item.get('parkCode')}")
        return data
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed for {endpoint}: {str(e)}")
        if hasattr(response, 'status_code') and response.status_code == 429:
            Logging.warning("Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return make_api_request(session, endpoint, params)
        return {"data": [], "total": 0}

def update(configuration: dict, state: dict):
    """Retrieve data from the NPS API."""
    session = create_retry_session()
    try:
        API_KEY = get_api_key(configuration)
        BASE_URL = "https://developer.nps.gov/api/v1"
        
        # List of all National Park codes
        park_codes = [
            'acad', 'arch', 'badl', 'bibe', 'bisc', 'blca', 'brca', 'cany', 'care', 'cave',
            'chis', 'coga', 'crla', 'cuva', 'dena', 'deva', 'drto', 'ever', 'gaar', 'glac',
            'glba', 'grba', 'grca', 'grsm', 'grte', 'gumo', 'hale', 'havo', 'hosp', 'isro',
            'jeff', 'jotr', 'kefj', 'kova', 'lacl', 'lavo', 'maca', 'meve', 'mora', 'noca',
            'npsa', 'olym', 'pefo', 'pinn', 'redw', 'romo', 'sagu', 'seki', 'shen', 'thro',
            'voya', 'wica', 'wrst', 'yell', 'yose', 'zion'
        ]
        
        # Get all National Parks using individual requests
        Logging.warning("Starting main parks sync")
        all_parks = []
        
        # Request each park individually
        for park_code in park_codes:
            base_params = {
                "api_key": API_KEY,
                "parkCode": park_code
            }
            
            Logging.warning(f"Requesting park with code: {park_code}")
            parks_response = make_api_request(session, f"{BASE_URL}/parks", base_params)
            parks_data = parks_response.get("data", [])
            
            for park in parks_data:
                designation = park.get("designation", "")
                # Include variations of National Park designations
                if (designation == "National Park" or
                    designation == "National Park & Preserve" or
                    designation == "National Parks" or
                    "National Park" in designation):  # This will catch combined designations
                    all_parks.append(park)
                    Logging.warning(f"Found National Park: {park.get('fullName')} | State(s): {park.get('states', 'N/A')} | Designation: {designation}")
            
            # Small delay to be nice to the API
            time.sleep(0.1)
        
        Logging.warning(f"Final count of National Parks: {len(all_parks)}")
        
        # Process parks
        for park in all_parks:
            try:
                yield op.upsert(
                    table="parks",
                    data={
                        "park_id": park.get("id", "Unknown ID"),
                        "name": park.get("fullName", "No Name"),
                        "description": park.get("description", "No Description"),
                        "state": park.get("states", ""),
                        "latitude": float(park.get("latitude")) if park.get("latitude") else None,
                        "longitude": float(park.get("longitude")) if park.get("longitude") else None,
                        "activities": json.dumps([activity["name"] for activity in park.get("activities", [])]),
                        "designation": park.get("designation", "")
                    }
                )
            except Exception as e:
                Logging.warning(f"Error processing park {park.get('id', 'Unknown')}: {str(e)}")
                continue

        # Sync fees/passes for National Parks
        Logging.warning("Starting fees/passes sync")
        for park in all_parks:
            park_id = park.get("id", "Unknown ID")
            park_name = park.get("fullName", "Unknown Park")
            
            # Process entrance fees
            for fee in park.get("entranceFees", []):
                try:
                    yield op.upsert(
                        table="feespasses",
                        data={
                            "pass_id": fee.get("id", "Unknown ID"),
                            "park_id": park_id,
                            "park_name": park_name,
                            "title": fee.get("title", "No Title"),
                            "cost": float(fee.get("cost", 0)),
                            "description": fee.get("description", ""),
                            "valid_for": "Fee"
                        }
                    )
                except Exception as e:
                    Logging.warning(f"Error processing fee for park {park_id}: {str(e)}")
                    continue

            # Process entrance passes
            for pass_item in park.get("entrancePasses", []):
                try:
                    yield op.upsert(
                        table="feespasses",
                        data={
                            "pass_id": pass_item.get("id", "Unknown ID"),
                            "park_id": park_id,
                            "park_name": park_name,
                            "title": pass_item.get("title", "No Title"),
                            "cost": float(pass_item.get("cost", 0)),
                            "description": pass_item.get("description", ""),
                            "valid_for": "Pass"
                        }
                    )
                except Exception as e:
                    Logging.warning(f"Error processing pass for park {park_id}: {str(e)}")
                    continue

        # Sync things to do for National Parks
        Logging.warning("Starting things to do sync")
        for park in all_parks:
            park_id = park.get("id", "Unknown ID")
            params = {
                "api_key": API_KEY,
                "parkCode": park.get("parkCode")
            }
            
            thingstodo_response = make_api_request(session, f"{BASE_URL}/thingstodo", params)
            for activity in thingstodo_response.get("data", []):
                try:
                    yield op.upsert(
                        table="thingstodo",
                        data={
                            "activity_id": activity.get("id", "Unknown ID"),
                            "park_id": park_id,
                            "park_name": park.get("fullName", "Unknown Park"),
                            "park_state": park.get("states", ""),
                            "title": activity.get("title", "No Title"),
                            "short_description": activity.get("shortDescription", ""),
                            "accessibility_information": activity.get("accessibilityInformation", ""),
                            "location": activity.get("location", ""),
                            "url": activity.get("url", ""),
                            "duration": activity.get("duration", ""),
                            "tags": json.dumps(activity.get("tags", []))
                        }
                    )
                except Exception as e:
                    Logging.warning(f"Error processing activity for park {park_id}: {str(e)}")
                    continue
            
            time.sleep(0.1)  # Small delay between parks

        yield op.checkpoint(state={})

    except Exception as e:
        Logging.warning(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting NPS connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")