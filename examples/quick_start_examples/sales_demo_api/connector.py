from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
import time
from datetime import datetime, timezone

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    # Validate required configuration parameters
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return []

    return [
        {
            "table": "qbr_records",
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
    # Validate required configuration parameters
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return

    base_url = configuration.get('base_url', 'https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com')
    page_size = int(configuration.get('page_size', '100'))

    # Ensure page_size is within API limits (1-200)
    page_size = min(max(page_size, 1), 200)

    # Setup API client
    headers = {"api_key": api_key}
    session = requests.Session()
    session.headers.update(headers)

    # Get the stored cursor from the previous sync
    cursor = state.get('next_cursor')

    # Get the timestamp for this sync
    current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Track record count for logging
    record_count = 0
    page_count = 0

    try:
        has_more_pages = True

        while has_more_pages:
            page_count += 1
            log.info(f"Fetching page {page_count} of QBR records" + (f" with cursor: {cursor}" if cursor else ""))

            # Build request parameters
            params = {"page_size": page_size}
            if cursor:
                params["cursor"] = cursor

            # Make API request with exponential backoff for rate limiting
            max_retries = 5
            retry_count = 0
            backoff_time = 1

            while retry_count < max_retries:
                try:
                    response = session.get(f"{base_url}/qbr_data", params=params)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if e.response and e.response.status_code == 429:
                        # Rate limited - get retry time from headers or use exponential backoff
                        retry_after = int(e.response.headers.get('Retry-After', backoff_time))
                        log.warning(f"Rate limited. Waiting for {retry_after} seconds before retry {retry_count}/{max_retries}")
                        time.sleep(retry_after)
                        backoff_time *= 2  # Exponential backoff
                    elif retry_count < max_retries:
                        log.warning(f"API request failed: {str(e)}. Retrying in {backoff_time} seconds ({retry_count}/{max_retries})")
                        time.sleep(backoff_time)
                        backoff_time *= 2  # Exponential backoff
                    else:
                        log.severe(f"Failed to fetch QBR data after {max_retries} retries: {str(e)}")
                        return

            # If we exceeded retries, exit
            if retry_count >= max_retries:
                log.severe(f"Exceeded maximum retry attempts ({max_retries})")
                return

            # Process the response
            data = response.json()
            qbr_records = data.get('qbr_records', [])

            # Process each record
            for record in qbr_records:
                # Ensure all float values are properly handled
                record_count += 1

                # Normalizing float values to avoid type errors
                for float_field in ['feature_adoption_rate', 'avg_resolution_time_hours', 'csat_score', 'sla_compliance_rate', 'health_score']:
                    if float_field in record:
                        try:
                            record[float_field] = float(record[float_field]) if record[float_field] is not None else None
                        except (TypeError, ValueError):
                            record[float_field] = None

                # Yield update operation
                yield op.update("qbr_records", record)

            # Check if we have more pages
            next_cursor = data.get('next_cursor')
            has_more_pages = bool(next_cursor)

            # Save cursor for next page or next sync
            if next_cursor:
                cursor = next_cursor

                # Create a checkpoint every 10 pages to save progress
                if page_count % 10 == 0:
                    log.info(f"Checkpointing after {record_count} records")
                    yield op.checkpoint({"next_cursor": cursor, "last_sync_timestamp": current_timestamp})

        # Final checkpoint after all records are processed
        log.info(f"Completed sync with {record_count} records processed across {page_count} pages")
        yield op.checkpoint({"next_cursor": cursor, "last_sync_timestamp": current_timestamp})

    except Exception as e:
        log.severe(f"Unexpected error during sync: {str(e)}")

# Create the connector instance
connector = Connector(update=update, schema=schema)

# Main entry point for local debugging
if __name__ == "__main__":
    connector.debug()