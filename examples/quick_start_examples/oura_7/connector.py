from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
import requests
from datetime import datetime, timedelta
import time

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
            "table": "daily_activity",
            "primary_key": ["id"]
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the Oura API and yield operations"""

    # 1. Validate configuration
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return

    # 2. Set up session
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    })

    # 3. Retrieve last state or use March 1, 2025 as start date
    next_cursor = state.get('next_cursor')
    start_date = state.get('start_date', "2025-03-01")

    # 4. Pagination setup
    url = "https://api.ouraring.com/v2/usercollection/daily_activity"
    params = {
        "start_date": start_date,
        "end_date": datetime.now().strftime("%Y-%m-%d"),
        "page_size": 100
    }
    
    if next_cursor:
        params["next_token"] = next_cursor

    record_count = 0
    has_more = True

    try:
        while has_more:
            try:
                log.info(f"Fetching data with params: {params}")
                response = session.get(url, params=params)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    log.info(f"Rate limited. Waiting for {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                data = response.json()

                records = data.get("data", [])
                for record in records:
                    yield op.upsert("daily_activity", record)
                    record_count += 1

                    # Checkpoint after every 100 records
                    if record_count % 100 == 0:
                        next_cursor = data.get("next_token")
                        if next_cursor:
                            yield op.checkpoint({
                                "next_cursor": next_cursor,
                                "start_date": start_date
                            })
                            log.info(f"Checkpoint saved after {record_count} records")

                # Check if there are more pages
                next_cursor = data.get("next_token")
                has_more = next_cursor is not None
                if has_more:
                    params["next_token"] = next_cursor
                else:
                    log.info("No more pages to fetch")
                    
                    # Update start_date for the next sync to be the day after end_date
                    next_start = (datetime.strptime(params["end_date"], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    
                    # Final checkpoint with updated start_date
                    yield op.checkpoint({
                        "next_cursor": None,
                        "start_date": next_start
                    })
                    log.info(f"Final checkpoint saved with next start date: {next_start}")

            except requests.exceptions.RequestException as e:
                log.severe(f"API request failed: {str(e)}")
                if hasattr(e.response, 'text'):
                    log.severe(f"Response: {e.response.text}")
                break
            
            # Avoid hitting rate limits
            time.sleep(1)

    except Exception as e:
        log.severe(f"Unexpected error: {str(e)}")

# Create the connector
connector = Connector(update=update, schema=schema)

# For debugging
if __name__ == "__main__":
    connector.debug()