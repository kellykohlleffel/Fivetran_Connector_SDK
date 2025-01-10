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

def get_credentials(configuration):
    """Retrieve the credentials from the configuration."""
    try:
        client_id = configuration.get('client_id')
        client_secret = configuration.get('client_secret')
        if not client_id or not client_secret:
            raise KeyError("Missing client_id or client_secret in configuration")
        return str(client_id), str(client_secret)
    except Exception as e:
        raise KeyError(f"Error retrieving credentials: {str(e)}")

def get_auth_token(client_id, client_secret):
    """Get OAuth token from Petfinder API"""
    try:
        auth_url = "https://api.petfinder.com/v2/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        response = rq.post(auth_url, data=data)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        Logging.warning(f"Error getting auth token: {str(e)}")
        raise

def schema(configuration: dict):
    """Define the table schema for Fivetran."""
    return [
        {
            "table": "dogs",
            "primary_key": ["id"],
            "columns": {}  # Let Fivetran infer data types
        }
    ]

def make_api_request(session, endpoint, headers, params=None):
    """Make API request with error handling and logging"""
    try:
        base_url = "https://api.petfinder.com/v2"
        full_url = f"{base_url}{endpoint}"
        
        log_params = params.copy() if params else {}
        
        Logging.warning(f"Making request to {endpoint} with params: {log_params}")
        response = session.get(full_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        return data
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed for {endpoint}: {str(e)}")
        if hasattr(response, 'status_code'):
            if response.status_code == 429:
                Logging.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                return make_api_request(session, endpoint, headers, params)
            elif response.status_code == 401:
                Logging.warning("Authentication token expired, please refresh")
                raise
        return {"animals": []}

def update(configuration: dict, state: dict):
    """Retrieve dog data from the Petfinder API."""
    session = create_retry_session()
    try:
        client_id, client_secret = get_credentials(configuration)
        auth_token = get_auth_token(client_id, client_secret)
        
        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
        
        Logging.warning("Starting sync for dogs")
        total_dogs_processed = 0
        
        # Get dogs with pagination
        page = 1
        while page <= 5:  # Limit to 5 pages to manage API calls
            params = {
                "type": "dog",
                "page": page,
                "limit": 100,  # Maximum allowed by API
                "sort": "recent"  # Get most recently added/updated dogs
            }
            
            data = make_api_request(session, "/animals", headers, params)
            dogs = data.get("animals", [])
            
            if not dogs:
                break
            
            page_dog_count = len(dogs)
            Logging.warning(f"Processing {page_dog_count} dogs from page {page}")
            
            for dog in dogs:
                # Extract breeds data
                breeds = dog.get("breeds", {})
                
                # Process dog data with simplified schema
                dog_data = {
                    "id": dog.get("id"),
                    "name": dog.get("name"),
                    "age": dog.get("age"),
                    "gender": dog.get("gender"),
                    "size": dog.get("size"),
                    "coat": dog.get("coat"),
                    "status": dog.get("status"),
                    "primary_breed": breeds.get("primary"),
                    "secondary_breed": breeds.get("secondary"),
                    "mixed_breed": breeds.get("mixed", False),
                    "colors_primary": dog.get("colors", {}).get("primary"),
                    "colors_secondary": dog.get("colors", {}).get("secondary"),
                    "colors_tertiary": dog.get("colors", {}).get("tertiary"),
                    "organization_id": dog.get("organization_id"),
                    "description": dog.get("description"),
                    "tags": json.dumps(dog.get("tags", [])),
                    "city": dog.get("contact", {}).get("address", {}).get("city"),
                    "state": dog.get("contact", {}).get("address", {}).get("state"),
                    "distance": dog.get("distance"),
                    "published_at": dog.get("published_at"),
                    "last_updated": datetime.utcnow().isoformat()
                }
                
                yield op.upsert(
                    table="dogs",
                    data=dog_data
                )
            
            total_dogs_processed += page_dog_count
            Logging.warning(f"Total dogs processed so far: {total_dogs_processed}")
            
            if page_dog_count < 100:  # If we get less than the maximum, we've reached the end
                break
                
            page += 1
            time.sleep(1)  # Rate limiting between pages
        
        Logging.warning(f"Sync complete. Processed {total_dogs_processed} dogs")
        yield op.checkpoint(state={})
        
    except Exception as e:
        Logging.warning(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting Petfinder dogs connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")