import requests
import time
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    # Validate API key is provided
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return []

    return [
        {
            "table": "qbr_data",
            "primary_key": ["record_id"],
            "columns": {
                "record_id": "STRING",
                "company_id": "STRING",
                "company_name": "STRING",
                "industry": "STRING",
                "size": "STRING",
                "contract_value": "INT",
                "contract_start_date": "STRING",
                "contract_expiration_date": "STRING",
                "qbr_quarter": "STRING",
                "qbr_year": "INT",
                "deal_stage": "STRING",
                "renewal_probability": "INT",
                "upsell_opportunity": "INT",
                "active_users": "INT",
                "feature_adoption_rate": "FLOAT",
                "custom_integrations": "INT",
                "pending_feature_requests": "INT",
                "ticket_volume": "INT",
                "avg_resolution_time_hours": "FLOAT",
                "csat_score": "FLOAT",
                "sla_compliance_rate": "FLOAT",
                "success_metrics_defined": "STRING",
                "roi_calculated": "STRING",
                "estimated_roi_value": "STRING",
                "economic_buyer_identified": "STRING",
                "executive_sponsor_engaged": "STRING",
                "decision_maker_level": "STRING",
                "decision_process_documented": "STRING",
                "next_steps_defined": "STRING",
                "decision_timeline_clear": "STRING",
                "technical_criteria_met": "STRING",
                "business_criteria_met": "STRING",
                "success_criteria_defined": "STRING",
                "pain_points_documented": "STRING",
                "pain_impact_level": "STRING",
                "urgency_level": "STRING",
                "champion_identified": "STRING",
                "champion_level": "STRING",
                "champion_engagement_score": "INT",
                "competitive_situation": "STRING",
                "competitive_position": "STRING",
                "health_score": "FLOAT"
            }
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the QBR Data API and yield operations"""
    # 1. Validate required configuration parameters
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return

    base_url = configuration.get('base_url', 'https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com')
    page_size = int(configuration.get('page_size', '100'))
    max_retries = int(configuration.get('max_retries', '5'))

    # 2. Setup API client with configuration
    headers = {"api_key": api_key}
    session = requests.Session()
    session.headers.update(headers)

    # 3. Current timestamp for checkpoint
    current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. Initialize sync variables
    endpoint = f"{base_url}/qbr_data"
    cursor = state.get('next_cursor', None)
    params = {"page_size": page_size}
    if cursor:
        params["cursor"] = cursor

    record_count = 0
    page_count = 0
    checkpoint_interval = 10  # Check point every 10 pages or ~1000 records

    # 5. Fetch data with pagination
    try:
        has_more = True
        while has_more:
            # Make API request with retries and backoff
            retry_count = 0
            response = None

            while retry_count < max_retries:
                try:
                    log.info(f"Fetching QBR data page{f' with cursor {cursor}' if cursor else ''}")
                    response = session.get(endpoint, params=params)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        log.severe(f"Failed to fetch data after {max_retries} retries: {str(e)}")
                        return

                    wait_time = min(2 ** retry_count, 60)  # Exponential backoff capped at 60 seconds
                    log.warning(f"Request failed, retrying in {wait_time} seconds: {str(e)}")
                    time.sleep(wait_time)

            # Process API response
            if not response:
                log.severe("Failed to get a valid response from the API")
                return

            data = response.json()
            records = data.get('qbr_records', [])

            # Process each record
            for record in records:
                record_count += 1
                yield op.update("qbr_data", record)

            # Update pagination info
            cursor = data.get('next_cursor')
            has_more = cursor is not None

            if has_more:
                params["cursor"] = cursor

            # Regular checkpoints
            page_count += 1
            if page_count % checkpoint_interval == 0:
                log.info(f"Checkpointing after {record_count} records")
                yield op.checkpoint({"next_cursor": cursor, "last_sync_timestamp": current_timestamp})

        # Final checkpoint
        log.info(f"Completed sync of {record_count} QBR records")
        yield op.checkpoint({"next_cursor": None, "last_sync_timestamp": current_timestamp})

    except Exception as e:
        log.severe(f"Unexpected error during sync: {str(e)}")

# Create connector object
connector = Connector(update=update, schema=schema)

# Entry point for local testing
if __name__ == "__main__":
    connector.debug()