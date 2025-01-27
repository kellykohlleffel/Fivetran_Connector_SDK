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
        total=5,
        backoff_factor=2,
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
    """Define the table schemas for Fivetran."""
    return [
        {
            "table": "crude_reserves_production",
            "primary_key": ["period", "series"],
            "columns": {
                "period": "STRING",
                "series": "STRING",
                "value": "FLOAT",
                "area_name": "STRING",
                "description": "STRING",
                "units": "STRING",
                "last_updated": "STRING"
            }
        },
        {
            "table": "crude_imports",
            "primary_key": ["period", "series"],
            "columns": {
                "period": "STRING",
                "series": "STRING",
                "value": "FLOAT",
                "area_name": "STRING",
                "description": "STRING",
                "units": "STRING",
                "last_updated": "STRING"
            }
        }
    ]


def make_api_request(session, api_key, route, params=None):
    """Make API request with error handling and logging"""
    base_url = "https://api.eia.gov/v2"
    
    if params is None:
        params = {}
    
    params['api_key'] = api_key
    log_params = params.copy()
    log_params['api_key'] = '***'  # Hide API key in logs
    Logging.warning(f"Making request to {route} with params: {log_params}")

    try:
        url = f"{base_url}/{route}/data"
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed: {str(e)}")
        raise


def process_data(data, table_name):
    """Process data from the EIA API response."""
    processed_records = []
    
    for record in data.get('response', {}).get('data', []):
        value = record.get('value', 0.0)
        try:
            value = float(value) if value else 0.0
        except (ValueError, TypeError):
            value = 0.0
            
        processed_record = {
            'period': str(record.get('period', '')),
            'series': str(record.get('series', '')),
            'value': value,
            'area_name': str(record.get('area-name', '')),
            'description': str(record.get('series-description', '')),
            'units': str(record.get('units', '')).replace('\\/', '/'),
            'last_updated': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        processed_records.append(processed_record)
    
    return processed_records


def update(configuration: dict, state: dict):
    """Retrieve the most recent petroleum data from the EIA API."""
    session = create_retry_session()
    
    try:
        api_key = get_api_key(configuration)
        # Read the record limit from the configuration, default to 500 if not set
        record_limit = int(configuration.get("record_limit", 500))
        
        routes = [
            {
                'path': 'petroleum/crd/pres',
                'table': 'crude_reserves_production',
                'frequency': 'annual',
                'length': record_limit
            },
            {
                'path': 'petroleum/move/imp',
                'table': 'crude_imports',
                'frequency': 'monthly',
                'length': record_limit
            }
        ]

        for route in routes:
            Logging.warning(f"Starting sync for {route['table']} with a record limit of {record_limit}")
            
            # Fetch most recent records
            params = {
                'frequency': route['frequency'],
                'start': '2014-01',  # Ensure a reasonable start date
                'end': datetime.now().strftime('%Y-%m'),  # Current date
                'sort[0][column]': 'period',  # Sort by period
                'sort[0][direction]': 'desc',  # Descending order (most recent first)
                'data[0]': 'value',  # Request value field
                'offset': 0,  # Start from the first batch
                'length': route['length']  # Fetch only up to record_limit
            }

            try:
                # Make API request
                data = make_api_request(session, api_key, route['path'], params)
                processed_records = process_data(data, route['table'])

                # Process and upsert records
                for record in processed_records:
                    yield op.upsert(
                        table=route['table'],
                        data=record
                    )
                
                Logging.warning(f"Processed {len(processed_records)} records for {route['table']}")

                # Checkpoint after sync
                yield op.checkpoint({
                    "last_sync_date": datetime.utcnow().strftime('%Y-%m')
                })

            except Exception as e:
                Logging.warning(f"Error processing {route['table']}: {str(e)}")
                raise

        Logging.warning("Sync complete")
        
    except Exception as e:
        Logging.warning(f"Error during sync: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting EIA petroleum connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")