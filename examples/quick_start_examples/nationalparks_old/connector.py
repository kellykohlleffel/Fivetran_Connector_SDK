"""
connector.py

This script connects to the National Park Service (NPS) API using the Fivetran Connector SDK.
It retrieves data on U.S. National Parks, articles, fees and passes, people, and alerts.
The data is stored in Fivetran using the SDK's upsert operation.

Example usage: This script demonstrates pulling park, article, feespasses, people, and alerts data from the NPS API, useful for 
analyzing park details, alerts, fees, passes, associated articles, and historical figures.

Configuration:
- An API key is required for accessing the NPS API. Replace 'YOUR_API_KEY' in the `API_KEY` variable
  with your actual API key.
- Set the `LIMIT` variable to control the number of records retrieved per table.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.
"""

import json
import os
import time
from datetime import datetime, timedelta
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

def get_api_key():
    """Get NPS API key from configuration"""
    try:
        # First try local configuration.json (Fivetran deployment)
        local_config_path = os.path.join(os.path.dirname(__file__), 'configuration.json')
        if os.path.exists(local_config_path):
            with open(local_config_path) as f:
                config = json.load(f)
                if config.get('nps_api_key'):
                    return config['nps_api_key']

        # Fallback to config.json in root (local development)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        config_path = os.path.join(root_dir, 'config.json')
        with open(config_path) as f:
            config = json.load(f)
        return config['apis']['nps']['api_key']
    except Exception as e:
        Logging.info(f"Error reading configuration: {str(e)}")
        raise

def get_default_state():
    """Return default state with timestamps 30 days ago"""
    default_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "articles_last_sync": default_date,
        "alerts_last_sync": default_date
    }

def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
    
    Returns:
        list: A list with schema definitions for each table to sync.
    """
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
            },
        },
        {
            "table": "articles",
            "primary_key": ["article_id"],
            "columns": {
                "article_id": "STRING",
                "title": "STRING",
                "url": "STRING",
                "related_parks": "STRING",
                "park_names": "STRING",
                "states": "STRING",
                "listing_description": "STRING",
                "date": "STRING",
            },
        },
        {
            "table": "feespasses",
            "primary_key": ["pass_id"],
            "columns": {
                "pass_id": "STRING",
                "park_id": "STRING",
                "title": "STRING",
                "cost": "FLOAT",
                "description": "STRING",
                "valid_for": "STRING",
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
            },
        },
        {
            "table": "alerts",
            "primary_key": ["alert_id"],
            "columns": {
                "alert_id": "STRING",
                "park_id": "STRING",
                "title": "STRING",
                "description": "STRING",
                "category": "STRING",
                "url": "STRING",
            },
        }
    ]

def make_api_request(session, endpoint, params):
    """Centralized API request handling with error handling and logging"""
    try:
        response = session.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except rq.exceptions.RequestException as e:
        Logging.info(f"API request failed for {endpoint}: {str(e)}")
        if hasattr(response, 'status_code') and response.status_code == 429:
            time.sleep(60)
            return make_api_request(session, endpoint, params)
        return {"data": [], "total": 0}

def update(configuration: dict, state: dict):
    """Retrieve data from the NPS API."""
    session = create_retry_session()
    API_KEY = get_api_key()  # Remove the configuration parameter here
    LIMIT = 50

    # Initialize or get state
    if not state:
        state = get_default_state()
    
    base_params = {
        "api_key": API_KEY,
        "limit": LIMIT
    }

    try:
        # Parks and related fees/passes
        parks_response = make_api_request(session, "https://developer.nps.gov/api/v1/parks", base_params)
        parks_data = parks_response.get("data", [])
        
        Logging.info(f"Retrieved {len(parks_data)} parks")
        
        for park in parks_data:
            try:
                park_id = park.get("id", "Unknown ID")
                
                # Parks table
                yield op.upsert(
                    table="parks",
                    data={
                        "park_id": park_id,
                        "name": park.get("fullName", "No Name"),
                        "description": park.get("description", "No Description"),
                        "state": ", ".join(park.get("states", [])),
                        "latitude": float(park.get("latitude")) if park.get("latitude") else None,
                        "longitude": float(park.get("longitude")) if park.get("longitude") else None,
                        "activities": ", ".join(activity["name"] for activity in park.get("activities", [])),
                    }
                )

                # Process fees and passes
                for fee in park.get("entranceFees", []):
                    try:
                        yield op.upsert(
                            table="feespasses",
                            data={
                                "pass_id": fee.get("id", "Unknown ID"),
                                "park_id": park_id,
                                "title": fee.get("title", "No Title"),
                                "cost": float(fee.get("cost", 0)),
                                "description": fee.get("description", "No Description"),
                                "valid_for": fee.get("validFor", ""),
                            }
                        )
                    except Exception as e:
                        Logging.info(f"Error processing fee for park {park_id}: {str(e)}")
                        continue

                for pass_item in park.get("entrancePasses", []):
                    try:
                        yield op.upsert(
                            table="feespasses",
                            data={
                                "pass_id": pass_item.get("id", "Unknown ID"),
                                "park_id": park_id,
                                "title": pass_item.get("title", "No Title"),
                                "cost": float(pass_item.get("cost", 0)),
                                "description": pass_item.get("description", "No Description"),
                                "valid_for": pass_item.get("validFor", ""),
                            }
                        )
                    except Exception as e:
                        Logging.info(f"Error processing pass for park {park_id}: {str(e)}")
                        continue

            except Exception as e:
                Logging.info(f"Error processing park {park_id}: {str(e)}")
                continue

        # People table
        people_response = make_api_request(session, "https://developer.nps.gov/api/v1/people", base_params)
        people_data = people_response.get("data", [])
        
        Logging.info(f"Retrieved {len(people_data)} people")
        
        for person in people_data:
            try:
                yield op.upsert(
                    table="people",
                    data={
                        "person_id": person.get("id", "Unknown ID"),
                        "name": person.get("title", "No Name"),
                        "title": person.get("listingDescription", "No Title"),
                        "description": person.get("listingDescription", "No Description"),
                        "url": person.get("url", ""),
                        "related_parks": ", ".join(park["parkCode"] for park in person.get("relatedParks", [])),
                    }
                )
            except Exception as e:
                Logging.info(f"Error processing person {person.get('id', 'Unknown')}: {str(e)}")
                continue

        # Articles - limited to most recent 50
        articles_response = make_api_request(session, "https://developer.nps.gov/api/v1/articles", base_params)
        articles_data = articles_response.get("data", [])
        
        Logging.info(f"Retrieved {len(articles_data)} articles")
        
        for article in articles_data:
            try:
                article_date = article.get("lastIndexedDate", "")
                if article_date > state["articles_last_sync"]:
                    yield op.upsert(
                        table="articles",
                        data={
                            "article_id": article.get("id", "Unknown ID"),
                            "title": article.get("title", "No Title"),
                            "url": article.get("url", ""),
                            "related_parks": ", ".join(park.get("parkCode", "") for park in article.get("relatedParks", [])),
                            "park_names": ", ".join(park.get("fullName", "") for park in article.get("relatedParks", [])),
                            "states": ", ".join(park.get("states", "") for park in article.get("relatedParks", [])),
                            "listing_description": article.get("listingDescription", ""),
                            "date": article_date,
                        }
                    )
            except Exception as e:
                Logging.info(f"Error processing article {article.get('id', 'Unknown')}: {str(e)}")
                continue

        # Update articles last sync time
        state["articles_last_sync"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Alerts - limited to most recent 50
        alerts_response = make_api_request(session, "https://developer.nps.gov/api/v1/alerts", base_params)
        alerts_data = alerts_response.get("data", [])
        
        Logging.info(f"Retrieved {len(alerts_data)} alerts")
        
        for alert in alerts_data:
            try:
                alert_date = alert.get("lastIndexedDate", "")
                if alert_date > state["alerts_last_sync"]:
                    yield op.upsert(
                        table="alerts",
                        data={
                            "alert_id": alert.get("id", "Unknown ID"),
                            "park_id": alert.get("parkCode", ""),
                            "title": alert.get("title", "No Title"),
                            "description": alert.get("description", "No Description"),
                            "category": alert.get("category", "No Category"),
                            "url": alert.get("url", ""),
                        }
                    )
            except Exception as e:
                Logging.info(f"Error processing alert {alert.get('id', 'Unknown')}: {str(e)}")
                continue

        # Update alerts last sync time
        state["alerts_last_sync"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        Logging.info(f"Major error during sync: {str(e)}")
        raise

    yield op.checkpoint(state=state)

# Create the connector object for Fivetran
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    print("Running the NPS connector...")
    connector.debug()
    print("Connector run complete.")
