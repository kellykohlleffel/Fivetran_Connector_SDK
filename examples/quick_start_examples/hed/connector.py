import requests
import time
import json
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

def schema(configuration: dict):
    """Define the minimal table schema for Fivetran"""
    # Validate configuration
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return []

    # Return minimal schema with ONLY table name and primary key
    return [
        {
            "table": "hed_records",
            "primary_key": ["record_id"]
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the endpoint and yield operations"""
    
    # Validate configuration
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return
    
    base_url = configuration.get('base_url')
    if not base_url:
        log.severe("Base URL is missing from configuration")
        return
    
    page_size = int(configuration.get('page_size', '100'))
    
    # Add the x-api-key to the session headers
    headers = {"x-api-key": api_key}
    session = requests.Session()
    session.headers.update(headers)
    
    # Retrieve the state for change data capture
    next_cursor = state.get('next_cursor')
    
    # Set up the parameters for the API request
    url = f"{base_url}/hed_data"
    params = {"page_size": page_size}
    if next_cursor:
        params["cursor"] = next_cursor
        log.info(f"Starting sync from cursor: {next_cursor}")
    else:
        log.info("Starting initial sync")
    
    record_count = 0
    has_more = True
    iteration_count = 0
    
    try:
        while has_more and iteration_count < 200:
            iteration_count += 1
            
            try:
                # Make API request with retry logic
                for attempt in range(3):
                    try:
                        response = session.get(url, params=params)
                        response.raise_for_status()
                        break
                    except requests.exceptions.RequestException as e:
                        if attempt == 2:
                            raise
                        log.warning(f"Request failed (attempt {attempt + 1}/3): {str(e)}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                
                data = response.json()
                
                # Process records
                records = data.get("hed_records", [])
                for record in records:
                    # Ensure the record has an ID
                    if 'record_id' in record:
                        yield op.upsert("hed_records", record)
                        record_count += 1
                    else:
                        log.warning(f"Skipping record without ID: {record}")
                
                # Update pagination info
                next_cursor = data.get("next_cursor")
                has_more = data.get("has_more", False)

                # Checkpoint every pagination batch
                yield op.checkpoint({"next_cursor": next_cursor})
                log.info(f"Checkpoint at {record_count} records, cursor: {next_cursor}")
                
                if next_cursor:
                    params["cursor"] = next_cursor
                
                log.info(f"Processed batch: {len(records)} records, has_more: {has_more}")
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    log.severe("Authentication failed - check API key")
                    return
                elif e.response.status_code == 429:
                    log.warning("Rate limit hit, waiting 60 seconds")
                    time.sleep(60)
                    continue
                else:
                    log.severe(f"HTTP error: {e.response.status_code} - {e.response.text}")
                    break
            except requests.exceptions.RequestException as e:
                log.severe(f"API request failed: {str(e)}")
                break
            except Exception as e:
                log.severe(f"Unexpected error processing response: {str(e)}")
                break
        
        # Check if we hit the iteration limit
        if iteration_count >= 200 and has_more:
            yield op.checkpoint({"next_cursor": next_cursor})
            log.info(f"Reached maximum iterations (200). Checkpointed at {record_count} records, cursor: {next_cursor}")
        elif next_cursor:
            yield op.checkpoint({"next_cursor": next_cursor})
            log.info(f"Final checkpoint: {record_count} total records, cursor: {next_cursor}")
        else:
            log.info(f"Sync completed: {record_count} total records")
    
    except Exception as e:
        log.severe(f"Unexpected error in update function: {str(e)}")

# This creates the connector object that will use the update function defined in this connector.py file
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE
    connector.debug(configuration=configuration)