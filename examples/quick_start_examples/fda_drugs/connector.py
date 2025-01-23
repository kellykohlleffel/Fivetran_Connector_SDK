import json
import time
from datetime import datetime
import requests as rq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    api_key = configuration.get('api_key')
    if not api_key:
        raise KeyError("Missing api_key in configuration")
    return str(api_key)

def schema(configuration: dict):
    """Define the table schema for Fivetran."""
    return [
        {
            "table": "drug_applications",
            "primary_key": ["application_number"],
            "columns": {}  # Let Fivetran infer data types
        }
    ]

def make_api_request(session, api_key, search=None, limit=100, skip=0):
    """Make API request with error handling and logging"""
    base_url = "https://api.fda.gov/drug/drugsfda.json"
    params = {
        'api_key': api_key,
        'limit': limit,
        'skip': skip
    }
    if search:
        params['search'] = search

    log_params = params.copy()
    log_params['api_key'] = '***'  # Mask API key in logs
    Logging.warning(f"Making request with params: {log_params}")

    response = session.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def process_drug_application(application):
    """Process a single drug application record."""
    products = application.get('products', [])
    product_data = []

    for product in products:
        product_data.append({
            'product_number': product.get('product_number', ''),
            'dosage_form': product.get('dosage_form', ''),
            'route': product.get('route', ''),
            'marketing_status': product.get('marketing_status', ''),
            'active_ingredients': json.dumps(product.get('active_ingredients', []))
        })

    primary_product = product_data[0] if product_data else {}

    return {
        "application_number": application.get('application_number'),
        "sponsor_name": application.get('sponsor_name'),
        "application_type": application.get('application_type'),
        "product_count": len(products),
        "submission_type": application.get('submission_type', [None])[0],
        "submission_status": application.get('submission_status', ''),
        "submission_class_code": application.get('submission_class_code', [None])[0],
        "primary_dosage_form": primary_product.get('dosage_form', ''),
        "primary_route": primary_product.get('route', ''),
        "product_details": json.dumps(product_data),
        "last_updated": datetime.utcnow().isoformat()
    }

def update(configuration: dict, state: dict):
    """Retrieve drug application data from the openFDA API."""
    session = create_retry_session()
    try:
        api_key = get_api_key(configuration)

        Logging.warning("Starting full sync for drug applications")
        total_processed = 0
        max_records_per_route = 100  # Limit to 100 records per route

        # Define routes to process
        routes = [
            'openfda.route.exact:"ORAL"',
            'openfda.route.exact:"TOPICAL"',
            'openfda.route.exact:"INTRAVENOUS"',
            'openfda.route.exact:"INTRAMUSCULAR"',
            'openfda.route.exact:"SUBCUTANEOUS"',
            'openfda.route.exact:"OPHTHALMIC"',
            'openfda.route.exact:"NASAL"',
            'openfda.route.exact:"DENTAL"',
            'openfda.route.exact:"TRANSDERMAL"'
        ]

        for route_query in routes:
            route_processed = 0
            skip = 0
            limit = 100

            while route_processed < max_records_per_route:
                data = make_api_request(session, api_key, search=route_query, limit=limit, skip=skip)
                applications = data.get('results', [])

                if not applications:
                    break

                page_count = len(applications)
                route_name = route_query.split('"')[1]
                Logging.warning(f"Processing {page_count} {route_name} applications (skip={skip})")

                for application in applications:
                    drug_data = process_drug_application(application)
                    yield op.upsert(
                        table="drug_applications",
                        data=drug_data
                    )
                    total_processed += 1
                    route_processed += 1

                if page_count < limit or route_processed >= max_records_per_route:
                    break

                skip += limit
                time.sleep(1)  # Rate limiting between requests

            Logging.warning(f"Completed processing {route_processed} {route_name} applications")

        Logging.warning(f"Sync complete. Processed {total_processed} total drug applications")
    except Exception as e:
        Logging.warning(f"Error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting openFDA drugs connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")
