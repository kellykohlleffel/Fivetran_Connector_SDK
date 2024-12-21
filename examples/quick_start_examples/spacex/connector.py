"""
connector.py

This script connects to the National Park Service (NPS) API using the Fivetran Connector SDK.
It retrieves data on U.S. National Parks, fees and passes, and people.
"""

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
        # Directly get the API key from the configuration
        api_key = configuration.get('api_key')
        
        if not api_key:
            # If not found, try the nested path
            apis = configuration.get("apis", {})
            nps = apis.get("nps", {})
            api_key = nps.get("api_key")
        
        if not api_key:
            raise KeyError("No API key found in configuration")
        
        return str(api_key)
    
    except Exception as e:
        raise KeyError(f"Error retrieving API key: {str(e)}")

def schema(configuration: dict):
    """Define the table schemas for Fivetran."""
    return [
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
                "designation": "STRING",
                "last_modified": "UTC_DATETIME"
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
                "last_modified": "UTC_DATETIME"
            },
        },
        {
            "table": "people",
            "primary_key": ["person_id"],
            "columns": {
                "person_id": "STRING",
                "name": "STRING",
                "title": "STRING",
                "description": "STRING",
                "url": "STRING",
                "related_parks": "STRING",
                "park_names": "STRING",
                "last_modified": "UTC_DATETIME"
            },
        }
    ]

def make_api_request(session, endpoint, params):
    """Make API request with error handling and logging"""
    try:
        Logging.info(f"Making request to {endpoint}")
        response = session.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except rq.exceptions.RequestException as e:
        Logging.info(f"API request failed for {endpoint}: {str(e)}")
        if hasattr(response, 'status_code') and response.status_code == 429:
            Logging.info("Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return make_api_request(session, endpoint, params)
        return {"data": [], "total": 0}

def update(configuration: dict, state: dict):
    """Retrieve data from the NPS API."""
    session = create_retry_session()
    current_time = datetime.utcnow().isoformat() + "Z"
    
    try:
        API_KEY = get_api_key(configuration)
        LIMIT = 50
        BASE_URL = "https://developer.nps.gov/api/v1"

        base_params = {
            "api_key": API_KEY,
            "limit": LIMIT
        }

        # Print header for visual output in debug mode
        print("\n--- Processing National Parks Data ---")
        print(f"{'Park ID':<15} {'Name':<40} {'State':<10} {'Last Modified':<25}")
        print("-" * 90)

        # 1. Sync National Parks only
        Logging.info("Starting parks sync")
        parks_response = make_api_request(session, f"{BASE_URL}/parks", base_params)
        parks_data = [park for park in parks_response.get("data", []) 
                     if park.get("designation", "").lower().strip() == "national park"]
        
        Logging.info(f"Retrieved {len(parks_data)} national parks")
        
        # Store park names for reference
        park_names = {park.get("id"): park.get("fullName") for park in parks_data}
        
        for park in parks_data:
            try:
                # Print for debug visibility
                print(f"{park.get('id', 'Unknown'):<15} {park.get('fullName', 'No Name')[:39]:<40} {park.get('states', ''):<10} {current_time:<25}")

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
                        "designation": park.get("designation", ""),
                        "last_modified": current_time
                    }
                )
            except Exception as e:
                Logging.info(f"Error processing park {park.get('id', 'Unknown')}: {str(e)}")
                continue

        # 2. Sync people
        print("\n--- Processing People Data ---")
        print(f"{'Person ID':<15} {'Name':<40} {'Last Modified':<25}")
        print("-" * 80)
        
        Logging.info("Starting people sync")
        people_response = make_api_request(session, f"{BASE_URL}/people", base_params)
        people_data = people_response.get("data", [])
        
        Logging.info(f"Retrieved {len(people_data)} people")
        
        for person in people_data:
            try:
                print(f"{person.get('id', 'Unknown'):<15} {person.get('name', 'No Name')[:39]:<40} {current_time:<25}")
                
                # Filter related parks to only include National Parks
                related_park_ids = [park.get("parkCode", "") for park in person.get("relatedParks", [])
                                  if park.get("parkCode") in park_names]
                related_park_names = [park_names.get(park_id, "") for park_id in related_park_ids]
                
                if related_park_ids:  # Only include people associated with National Parks
                    yield op.upsert(
                        table="people",
                        data={
                            "person_id": person.get("id", "Unknown ID"),
                            "name": person.get("name", person.get("title", "No Name")),
                            "title": person.get("listingDescription", ""),
                            "description": person.get("description", ""),
                            "url": person.get("url", ""),
                            "related_parks": json.dumps(related_park_ids),
                            "park_names": json.dumps(related_park_names),
                            "last_modified": current_time
                        }
                    )
            except Exception as e:
                Logging.info(f"Error processing person {person.get('id', 'Unknown')}: {str(e)}")
                continue

        # 3. Sync fees/passes
        print("\n--- Processing Fees and Passes ---")
        print(f"{'Pass ID':<15} {'Title':<40} {'Cost':<10} {'Last Modified':<25}")
        print("-" * 90)
        
        Logging.info("Starting fees/passes sync")
        for park in parks_data:
            park_id = park.get("id", "Unknown ID")
            park_name = park.get("fullName", "Unknown Park")
            
            # Process entrance fees
            for fee in park.get("entranceFees", []):
                try:
                    print(f"{fee.get('id', 'Unknown'):<15} {fee.get('title', 'No Title')[:39]:<40} {float(fee.get('cost', 0)):<10.2f} {current_time:<25}")
                    
                    yield op.upsert(
                        table="feespasses",
                        data={
                            "pass_id": fee.get("id", "Unknown ID"),
                            "park_id": park_id,
                            "park_name": park_name,
                            "title": fee.get("title", "No Title"),
                            "cost": float(fee.get("cost", 0)),
                            "description": fee.get("description", ""),
                            "valid_for": "Fee",
                            "last_modified": current_time
                        }
                    )
                except Exception as e:
                    Logging.info(f"Error processing fee for park {park_id}: {str(e)}")
                    continue

            # Process entrance passes
            for pass_item in park.get("entrancePasses", []):
                try:
                    print(f"{pass_item.get('id', 'Unknown'):<15} {pass_item.get('title', 'No Title')[:39]:<40} {float(pass_item.get('cost', 0)):<10.2f} {current_time:<25}")
                    
                    yield op.upsert(
                        table="feespasses",
                        data={
                            "pass_id": pass_item.get("id", "Unknown ID"),
                            "park_id": park_id,
                            "park_name": park_name,
                            "title": pass_item.get("title", "No Title"),
                            "cost": float(pass_item.get("cost", 0)),
                            "description": pass_item.get("description", ""),
                            "valid_for": "Pass",
                            "last_modified": current_time
                        }
                    )
                except Exception as e:
                    Logging.info(f"Error processing pass for park {park_id}: {str(e)}")
                    continue

        yield op.checkpoint(state={"last_sync": current_time})

    except Exception as e:
        Logging.info(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.info("Starting NPS connector debug run...")

    try:
        # Get the project root directory (where your config file exists)
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        example_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Source config path (in project root)
        source_config_path = os.path.join(example_dir, 'files', 'nps_config.json')
        
        # Make sure the files directory exists in the test environment
        os.makedirs('files', exist_ok=True)
        
        # Destination config path (in test environment)
        dest_config_path = os.path.join('files', 'nps_config.json')

        # Copy the config file if it doesn't exist in the test environment
        if not os.path.exists(dest_config_path):
            Logging.info(f"Copying config from {source_config_path} to {dest_config_path}")
            with open(source_config_path, 'r') as source:
                config_data = json.load(source)
                with open(dest_config_path, 'w') as dest:
                    json.dump(config_data, dest, indent=4)
            Logging.info("Successfully copied configuration file")

        connector.debug()
        Logging.info("Debug run complete.")
        
        # Clean up - remove the copied config file
        if os.path.exists(dest_config_path):
            os.remove(dest_config_path)
            
    except Exception as e:
        Logging.info(f"Error during debug run: {str(e)}")
        raise