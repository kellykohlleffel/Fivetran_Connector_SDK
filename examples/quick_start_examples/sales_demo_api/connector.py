from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, Any
from datetime import datetime
from dateutil import parser
import requests
import time

def schema(configuration: Dict[str, Any]) -> list:
    """Define the table schema for the QBR data"""
    return [{
        "table": "qbr_data",
        "primary_key": ["company_id", "qbr_quarter", "qbr_year"],
        "columns": {
            "company_id": "STRING",
            "company_name": "STRING",
            "industry": "STRING",
            "size": "STRING",
            "contract_value": "NUMBER",
            "contract_start_date": "UTC_DATETIME",
            "contract_expiration_date": "UTC_DATETIME",
            "qbr_quarter": "STRING",
            "qbr_year": "STRING",
            "deal_stage": "STRING",
            "renewal_probability": "INT",
            "upsell_opportunity": "STRING",
            "active_users": "INT",
            "feature_adoption_rate": "FLOAT",
            "custom_integrations": "INT",
            "pending_feature_requests": "INT",
            "ticket_volume": "INT",
            "avg_resolution_time_hours": "FLOAT",
            "csat_score": "FLOAT",
            "sla_compliance_rate": "FLOAT",
            "success_metrics_defined": "BOOLEAN",
            "roi_calculated": "BOOLEAN",
            "estimated_roi_value": "STRING",
            "economic_buyer_identified": "BOOLEAN",
            "executive_sponsor_engaged": "BOOLEAN",
            "decision_maker_level": "STRING",
            "decision_process_documented": "BOOLEAN",
            "next_steps_defined": "BOOLEAN",
            "decision_timeline_clear": "BOOLEAN",
            "technical_criteria_met": "BOOLEAN",
            "business_criteria_met": "BOOLEAN",
            "success_criteria_defined": "STRING",
            "pain_points_documented": "STRING",
            "pain_impact_level": "STRING",
            "urgency_level": "STRING",
            "champion_identified": "BOOLEAN",
            "champion_level": "STRING",
            "champion_engagement_score": "INT",
            "competitive_situation": "STRING",
            "competitive_position": "STRING",
            "health_score": "FLOAT"
        }
    }]

def normalize_date(date_str: str) -> str:
    """Normalize date strings to RFC 3339 format"""
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            dt = parser.parse(date_str)
        else:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        log.warning(f"Error normalizing date {date_str}: {str(e)}")
        return None

def fetch_qbr_data(configuration: Dict[str, Any], cursor: str = None) -> Dict[str, Any]:
    """Fetch QBR data from the API with exponential backoff retry logic"""
    base_url = "https://sdk-demo-api-dot-internal-sales.uc.r.appspot.com/qbr_data"
    headers = {"api_key": configuration["api_key"]}
    params = {"page_size": "200"}

    if cursor:
        params["cursor"] = cursor

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count == max_retries:
                raise Exception(f"Failed to fetch QBR data after {max_retries} attempts: {str(e)}")
            wait_time = 2 ** retry_count
            log.warning(f"Request failed, retrying in {wait_time} seconds...")
            time.sleep(wait_time)

def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Update QBR data incrementally"""
    cursor = state.get("cursor", None)
    request_count = state.get("request_count", 0)

    try:
        while True:
            request_count += 1
            log.info(f"Fetching QBR data (request #{request_count})")

            response_data = fetch_qbr_data(configuration, cursor)
            records = response_data.get("qbr_records", [])

            if not records:
                log.info("No more records to process")
                break

            for record in records:
                # Normalize date fields
                record["contract_start_date"] = normalize_date(record.get("contract_start_date"))
                record["contract_expiration_date"] = normalize_date(record.get("contract_expiration_date"))

                # Convert boolean strings to actual booleans
                boolean_fields = [
                    "success_metrics_defined", "roi_calculated", "economic_buyer_identified",
                    "executive_sponsor_engaged", "decision_process_documented", "next_steps_defined",
                    "decision_timeline_clear", "technical_criteria_met", "business_criteria_met",
                    "champion_identified"
                ]

                for field in boolean_fields:
                    if field in record:
                        record[field] = str(record[field]).lower() == "true"

                yield op.upsert("qbr_data", record)

            cursor = response_data.get("next_cursor")

            # Save state after processing each page
            yield op.checkpoint({
                "cursor": cursor,
                "request_count": request_count
            })

            if not cursor:
                log.info("No more pages to fetch")
                break

    except Exception as e:
        log.severe(f"Error during QBR data sync: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()