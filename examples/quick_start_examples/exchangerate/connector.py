import json
import os
import time
from datetime import datetime, timedelta
import requests as rq
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op

# Define major currencies to track - USD as base with four major pairs
MAJOR_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD']
BASE_CURRENCY = 'USD'

# Note: This connector uses the free tier of ExchangeRate API which provides:
# - Real-time current exchange rates
# - Single base currency (USD)
# Historical data is simulated based on current rates with small variations

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
    try:
        api_key = configuration.get('api_key')
        if not api_key:
            raise KeyError("No API key found in configuration")
        return str(api_key)
    except Exception as e:
        raise KeyError(f"Error retrieving API key: {str(e)}")

def schema(configuration: dict):
    """Define the table schemas for Fivetran."""
    return [
        {
            "table": "latest_rates",
            "primary_key": ["rate_id"],
            "columns": {
                "rate_id": "string",
                "currency_pair_id": "string",
                "base_currency": "string",
                "target_currency": "string",
                "exchange_rate": "string",
                "timestamp": "string",
                "last_updated": "string"
            }
        },
        {
            "table": "historical_rates",
            "primary_key": ["rate_id"],
            "columns": {
                "rate_id": "string",
                "currency_pair_id": "string",
                "base_currency": "string",
                "target_currency": "string",
                "exchange_rate": "string",
                "date": "string"
            }
        },
        {
            "table": "currency_pairs",
            "primary_key": ["currency_pair_id"],
            "columns": {
                "currency_pair_id": "string",
                "base_currency": "string",
                "target_currency": "string",
                "pair_name": "string",
                "is_major_pair": "string",
                "last_updated": "string"
            }
        }
    ]

def make_api_request(session, endpoint, api_key):
    """Make API request with error handling and logging"""
    try:
        base_url = "https://v6.exchangerate-api.com/v6"
        full_url = f"{base_url}/{api_key}/{endpoint}"
        
        Logging.warning(f"Making request to {endpoint}")
        response = session.get(full_url, timeout=30)
        
        # Check if we hit API limits
        if response.status_code == 429:
            Logging.warning("Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return make_api_request(session, endpoint, api_key)
            
        # For other error status codes
        if response.status_code != 200:
            Logging.warning(f"API request failed for {endpoint} with status code {response.status_code}")
            return None
            
        data = response.json()
        if data.get("result") != "success":
            Logging.warning(f"API request unsuccessful: {data.get('error-type', 'Unknown error')}")
            return None
            
        return data
    except Exception as e:
        Logging.warning(f"Exception during API request for {endpoint}: {str(e)}")
        return None

def get_latest_rates(session, api_key, base_currency):
    """Get latest exchange rates for base currency"""
    endpoint = f"latest/{base_currency}"
    return make_api_request(session, endpoint, api_key)

def get_historical_rates(session, api_key, base_currency, date):
    """Get historical exchange rates for base currency on specific date"""
    endpoint = f"history/{base_currency}/{date}"
    return make_api_request(session, endpoint, api_key)

def create_currency_pair_id(base, target):
    """Create a consistent currency pair identifier"""
    return f"{base}_{target}"

def create_rate_id(base, target, date_or_timestamp):
    """Create a consistent rate identifier"""
    return f"{base}_{target}_{date_or_timestamp}"

def update(configuration: dict, state: dict):
    """Retrieve and process exchange rate data.
    
    Data sourcing strategy:
    1. Real Data (API):
       - Current exchange rates for USD to EUR, GBP, JPY, AUD
       - Single API call to /latest/{base_currency}
       
    2. Simulated Data:
       - Historical rates for past 7 days
       - Based on current rates with small variations (±0.1% per day)
       
    3. Reference Data:
       - Currency pair metadata
       - Static information about currency relationships
    """
    session = create_retry_session()
    try:
        API_KEY = get_api_key(configuration)
        current_time = datetime.utcnow()
        
        # First, update currency pairs table
        Logging.warning("Updating currency pairs table...")
        for target_currency in MAJOR_CURRENCIES:
            if target_currency != BASE_CURRENCY:
                currency_pair_id = create_currency_pair_id(BASE_CURRENCY, target_currency)
                pair_name = f"{BASE_CURRENCY}/{target_currency}"
                
                yield op.upsert(
                    table="currency_pairs",
                    data={
                        "currency_pair_id": currency_pair_id,
                        "base_currency": BASE_CURRENCY,
                        "target_currency": target_currency,
                        "pair_name": pair_name,
                        "is_major_pair": "true",
                        "last_updated": current_time.isoformat()
                    }
                )
        
        # Get latest rates - single API call
        Logging.warning(f"Making single API call to get latest rates for {len(MAJOR_CURRENCIES)-1} currency pairs")
        latest_rates = get_latest_rates(session, API_KEY, BASE_CURRENCY)
        
        if latest_rates and latest_rates.get("result") == "success":
            rates = latest_rates.get("conversion_rates", {})
            timestamp = latest_rates.get("time_last_update_unix")
            
            for target_currency in MAJOR_CURRENCIES:
                if target_currency in rates and target_currency != BASE_CURRENCY:
                    currency_pair_id = create_currency_pair_id(BASE_CURRENCY, target_currency)
                    pair_name = f"{BASE_CURRENCY}/{target_currency}"
                    rate_value = str(rates[target_currency])
                    
                    Logging.warning(f"Processing {pair_name}: {rate_value}")
                    
                    # Insert into latest_rates
                    rate_id = create_rate_id(BASE_CURRENCY, target_currency, timestamp)
                    yield op.upsert(
                        table="latest_rates",
                        data={
                            "rate_id": rate_id,
                            "currency_pair_id": currency_pair_id,
                            "base_currency": BASE_CURRENCY,
                            "target_currency": target_currency,
                            "exchange_rate": rate_value,
                            "timestamp": str(timestamp),
                            "last_updated": current_time.isoformat()
                        }
                    )
                    
                    # Simulate historical data with realistic variations
                    today = datetime.now().date()
                    base_rate = float(rate_value)
                    
                    for i in range(7):
                        historical_date = today - timedelta(days=i+1)
                        date_str = historical_date.strftime('%Y-%m-%d')
                        
                        # Create a small random variation (±0.5%)
                        variation = (i + 1) * 0.001  # 0.1% change per day
                        if i % 2 == 0:
                            historical_rate = base_rate * (1 + variation)
                        else:
                            historical_rate = base_rate * (1 - variation)
                            
                        Logging.warning(f"Creating historical entry for {pair_name} on {date_str} with rate {historical_rate:.4f}")
                        
                        historical_rate_id = create_rate_id(BASE_CURRENCY, target_currency, date_str)
                        yield op.upsert(
                            table="historical_rates",
                            data={
                                "rate_id": historical_rate_id,
                                "currency_pair_id": currency_pair_id,
                                "base_currency": BASE_CURRENCY,
                                "target_currency": target_currency,
                                "exchange_rate": str(round(historical_rate, 4)),
                                "date": date_str
                            }
                        )
        
        # Update state
        yield op.checkpoint(state={
            "last_sync": current_time.isoformat()
        })
        
    except Exception as e:
        Logging.warning(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting ExchangeRate API connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")