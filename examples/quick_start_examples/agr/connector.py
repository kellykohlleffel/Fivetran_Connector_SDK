from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
import requests
from datetime import datetime, timezone

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
            "table": "agr_records",
            "primary_key": ["record_id"]
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the Agriculture API and yield operations"""

    # 1. Validate configuration
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return

    base_url = configuration.get('base_url', 'https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com')
    page_size = int(configuration.get('page_size', '100'))

    # 2. Set up session
    session = requests.Session()
    session.headers.update({"api_key": api_key})

    # 3. Retrieve last state
    next_cursor = state.get('next_cursor')

    # 4. Pagination setup
    url = f"{base_url}/agr_data"
    params = {"page_size": page_size}
    if next_cursor:
        params["cursor"] = next_cursor

    record_count = 0
    has_more = True

    try:
        while has_more:
            try:
                log.info(f"Fetching data with params: {params}")
                response = session.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                records = data.get("agr_records", [])
                for record in records:
                    yield op.upsert("agr_records", record)
                    record_count += 1

                    # Checkpoint after every 100 records
                    if record_count % 100 == 0:
                        next_cursor = data.get("next_cursor")
                        if next_cursor:
                            yield op.checkpoint({"next_cursor": next_cursor})
                            log.info(f"Checkpoint saved after {record_count} records")

                # Check if there are more pages
                next_cursor = data.get("next_cursor")
                has_more = next_cursor is not None
                if has_more:
                    params["cursor"] = next_cursor
                else:
                    log.info("No more pages to fetch")

            except requests.exceptions.RequestException as e:
                log.severe(f"API request failed: {str(e)}")
                break

        # Final checkpoint
        if next_cursor:
            yield op.checkpoint({"next_cursor": next_cursor})
            log.info(f"Final checkpoint saved. Total records processed: {record_count}")

    except Exception as e:
        log.severe(f"Unexpected error: {str(e)}")

# Create the connector
connector = Connector(update=update, schema=schema)

# For debugging
if __name__ == "__main__":
    connector.debug()