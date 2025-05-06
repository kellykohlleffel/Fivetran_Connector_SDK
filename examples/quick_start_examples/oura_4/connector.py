from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
from datetime import datetime, timedelta
import time
from urllib.parse import urljoin

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    return [
        {
            "table": "daily_activity",
            "primary_key": ["id"]
        },
        {
            "table": "daily_sleep",
            "primary_key": ["id"]
        }
    ]

def update(configuration: dict, state: dict):
    """Update function to fetch data from Oura API"""
    # Get configuration parameters
    api_token = configuration.get('api_token')
    if not api_token:
        log.severe("API token is not provided in the configuration")
        return

    base_url = configuration.get('base_url', 'https://api.ouraring.com/v2/')

    # Set up headers with authentication
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

    # Get last sync timestamps for each endpoint or use default
    default_start_date = "2025-03-01T00:00:00Z"  # March 1, 2025

    # Use the state to track last sync dates per table
    daily_activity_last_sync = state.get('daily_activity_last_sync', default_start_date)
    daily_sleep_last_sync = state.get('daily_sleep_last_sync', default_start_date)

    # Current time to use for this sync's state update
    current_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Sync daily activity data
    yield from sync_daily_activity(base_url, headers, daily_activity_last_sync, current_time)

    # Checkpoint after activity sync
    yield op.checkpoint({"daily_activity_last_sync": current_time})

    # Sync daily sleep data
    yield from sync_daily_sleep(base_url, headers, daily_sleep_last_sync, current_time)

    # Final checkpoint after all syncs
    yield op.checkpoint({"daily_sleep_last_sync": current_time})


def sync_daily_activity(base_url, headers, last_sync, current_time):
    """Sync daily activity data from Oura API"""
    log.info(f"Syncing daily activity data since {last_sync}")

    # Convert datetime strings to date strings for API
    start_date = datetime.strptime(last_sync, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
    end_date = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")

    endpoint = "usercollection/daily_activity"
    url = urljoin(base_url, endpoint)

    next_token = None
    page_count = 0
    last_checkpoint_time = time.time()

    while True:
        try:
            params = {
                "start_date": start_date,
                "end_date": end_date
            }

            if next_token:
                params["next_token"] = next_token

            log.info(f"Requesting daily activity data: {url} with params: {params}")
            response = requests.get(url, headers=headers, params=params)

            # Handle potential rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                log.warning(f"Rate limited. Waiting for {retry_after} seconds")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Process the data
            if 'data' in data and data['data']:
                for item in data['data']:
                    # Ensure all records have an id for primary key purposes
                    if 'id' not in item:
                        # Use date as id if not present
                        item['id'] = item.get('day', f"unknown_{time.time()}")

                    yield op.update("daily_activity", item)

            # Check for pagination
            next_token = data.get('next_token')
            page_count += 1

            # Log progress
            log.info(f"Processed page {page_count} of daily activity data with {len(data.get('data', []))} records")

            # Create a checkpoint every ~10 minutes of processing
            if time.time() - last_checkpoint_time > 600:
                yield op.checkpoint({"daily_activity_last_sync": current_time})
                last_checkpoint_time = time.time()
                log.info("Created checkpoint during daily activity sync")

            # Break if no more pages
            if not next_token:
                break

            # Be nice to the API - add a small delay between requests
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            log.severe(f"Error fetching daily activity data: {str(e)}")
            # For transient errors, we might want to retry with backoff
            if hasattr(e, 'response') and e.response and 500 <= e.response.status_code < 600:
                log.warning(f"Server error {e.response.status_code}, retrying in 30 seconds")
                time.sleep(30)
                continue
            else:
                # For client errors or other issues, stop the sync
                break


def sync_daily_sleep(base_url, headers, last_sync, current_time):
    """Sync daily sleep data from Oura API"""
    log.info(f"Syncing daily sleep data since {last_sync}")

    # Convert datetime strings to date strings for API
    start_date = datetime.strptime(last_sync, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
    end_date = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")

    endpoint = "usercollection/daily_sleep"
    url = urljoin(base_url, endpoint)

    next_token = None
    page_count = 0
    last_checkpoint_time = time.time()

    while True:
        try:
            params = {
                "start_date": start_date,
                "end_date": end_date
            }

            if next_token:
                params["next_token"] = next_token

            log.info(f"Requesting daily sleep data: {url} with params: {params}")
            response = requests.get(url, headers=headers, params=params)

            # Handle potential rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                log.warning(f"Rate limited. Waiting for {retry_after} seconds")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            # Process the data
            if 'data' in data and data['data']:
                for item in data['data']:
                    # Ensure all records have an id for primary key purposes
                    if 'id' not in item:
                        # Use date as id if not present
                        item['id'] = item.get('day', f"unknown_{time.time()}")

                    yield op.update("daily_sleep", item)

            # Check for pagination
            next_token = data.get('next_token')
            page_count += 1

            # Log progress
            log.info(f"Processed page {page_count} of daily sleep data with {len(data.get('data', []))} records")

            # Create a checkpoint every ~10 minutes of processing
            if time.time() - last_checkpoint_time > 600:
                yield op.checkpoint({"daily_sleep_last_sync": current_time})
                last_checkpoint_time = time.time()
                log.info("Created checkpoint during daily sleep sync")

            # Break if no more pages
            if not next_token:
                break

            # Be nice to the API - add a small delay between requests
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            log.severe(f"Error fetching daily sleep data: {str(e)}")
            # For transient errors, we might want to retry with backoff
            if hasattr(e, 'response') and e.response and 500 <= e.response.status_code < 600:
                log.warning(f"Server error {e.response.status_code}, retrying in 30 seconds")
                time.sleep(30)
                continue
            else:
                # For client errors or other issues, stop the sync
                break

# Create connector object
connector = Connector(update=update, schema=schema)

# Entry point for debugging
if __name__ == "__main__":
    connector.debug()