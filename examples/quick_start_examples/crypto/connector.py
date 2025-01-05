import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta
from collections import defaultdict
import time

# Top 10 cryptocurrencies to track
TOP_CRYPTOCURRENCIES = [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "solana",
    "ripple",
    "cardano",
    "dogecoin",
    "polkadot",
    "avalanche-2",
    "tron"
]

# API configuration
API_DELAY = 6  # 6 seconds between calls (10 calls per minute)
BATCH_SIZE = 3  # Process cryptocurrencies in smaller batches

def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.

    Args:
        configuration (dict): Configuration settings for the connector.

    Returns:
        list: Schema definitions for each table to sync.
    """
    return [
        {
            "table": "cryptocurrencies",
            "primary_key": ["id"]
        },
        {
            "table": "market_data",
            "primary_key": ["id"]
        }
    ]

def handle_rate_limit(response):
    """
    Handle rate limiting from the Coingecko API.
    
    Args:
        response (requests.Response): Response from the API
    
    Returns:
        bool: True if request should be retried, False otherwise
    """
    if response.status_code == 429:  # Too Many Requests
        retry_after = int(response.headers.get('Retry-After', 60))
        log.info(f"Rate limit hit. Waiting {retry_after} seconds...")
        time.sleep(retry_after)
        return True
    return False

def make_request(url, params=None, max_retries=3):
    """
    Make a request to the Coingecko API with retry logic.
    
    Args:
        url (str): The API endpoint URL
        params (dict): Query parameters
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        dict: JSON response data or None if failed
    """
    for attempt in range(max_retries):
        try:
            response = rq.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            
            if handle_rate_limit(response):
                continue
                
            if response.status_code != 200:
                log.error(f"API request failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            log.error(f"Request failed: {str(e)}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)  # Exponential backoff
            
    return None

def get_currency_details(currency_id):
    """
    Get detailed information for a specific cryptocurrency.
    
    Args:
        currency_id (str): The ID of the cryptocurrency
    
    Returns:
        dict: Cryptocurrency details
    """
    url = f"https://api.coingecko.com/api/v3/coins/{currency_id}"
    time.sleep(API_DELAY)  # Respect rate limits
    return make_request(url)

def get_market_data(currency_ids):
    """
    Get market data for specified cryptocurrencies.
    
    Args:
        currency_ids (list): List of cryptocurrency IDs
    
    Returns:
        list: Market data for specified cryptocurrencies
    """
    ids = ",".join(currency_ids)
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ids,
        "order": "market_cap_desc",
        "sparkline": False,
        "locale": "en"
    }
    time.sleep(API_DELAY)  # Respect rate limits
    return make_request(url, params) or []

def batch_currencies(currencies, batch_size):
    """
    Split currencies into smaller batches.
    
    Args:
        currencies (list): List of currency IDs
        batch_size (int): Size of each batch
    
    Returns:
        list: List of batches
    """
    return [currencies[i:i + batch_size] for i in range(0, len(currencies), batch_size)]

def update(configuration: dict, state: dict):
    """
    Retrieve data from the Coingecko API and send it to Fivetran.
    Limits the data to top 10 cryptocurrencies by market cap.
    
    Args:
        configuration (dict): Configuration settings for the connector.
        state (dict): Last sync state containing timestamps.
    """
    current_time = datetime.now()
    log.info(f"Fetching cryptocurrency data for top {len(TOP_CRYPTOCURRENCIES)} coins...")

    # Process cryptocurrency details in batches
    for currency_id in TOP_CRYPTOCURRENCIES:
        currency_details = get_currency_details(currency_id)
        
        if currency_details:
            crypto_data = {
                "id": currency_id,
                "name": currency_details.get("name"),
                "symbol": currency_details.get("symbol"),
                "block_time_in_minutes": currency_details.get("block_time_in_minutes"),
                "hashing_algorithm": currency_details.get("hashing_algorithm"),
                "description_en": currency_details.get("description", {}).get("en"),
                "genesis_date": currency_details.get("genesis_date"),
                "sentiment_votes_up_percentage": currency_details.get("sentiment_votes_up_percentage"),
                "sentiment_votes_down_percentage": currency_details.get("sentiment_votes_down_percentage"),
                "market_cap_rank": currency_details.get("market_cap_rank"),
                "coingecko_rank": currency_details.get("coingecko_rank"),
                "coingecko_score": currency_details.get("coingecko_score"),
                "developer_score": currency_details.get("developer_score"),
                "community_score": currency_details.get("community_score"),
                "liquidity_score": currency_details.get("liquidity_score"),
                "public_interest_score": currency_details.get("public_interest_score"),
                "last_updated": currency_details.get("last_updated")
            }
            
            yield op.upsert("cryptocurrencies", crypto_data)
            log.info(f"Processed cryptocurrency details for {currency_id}")
        else:
            log.error(f"Failed to fetch details for {currency_id}")

    # Process market data in batches
    for batch in batch_currencies(TOP_CRYPTOCURRENCIES, BATCH_SIZE):
        market_data = get_market_data(batch)
        
        if market_data:
            for data in market_data:
                market_entry = {
                    "id": f"{data.get('id')}_{current_time.isoformat()}",
                    "currency_id": data.get('id'),
                    "symbol": data.get('symbol'),
                    "name": data.get('name'),
                    "current_price": data.get('current_price'),
                    "market_cap": data.get('market_cap'),
                    "market_cap_rank": data.get('market_cap_rank'),
                    "fully_diluted_valuation": data.get('fully_diluted_valuation'),
                    "total_volume": data.get('total_volume'),
                    "high_24h": data.get('high_24h'),
                    "low_24h": data.get('low_24h'),
                    "price_change_24h": data.get('price_change_24h'),
                    "price_change_percentage_24h": data.get('price_change_percentage_24h'),
                    "market_cap_change_24h": data.get('market_cap_change_24h'),
                    "market_cap_change_percentage_24h": data.get('market_cap_change_percentage_24h'),
                    "circulating_supply": data.get('circulating_supply'),
                    "total_supply": data.get('total_supply'),
                    "max_supply": data.get('max_supply'),
                    "ath": data.get('ath'),
                    "ath_change_percentage": data.get('ath_change_percentage'),
                    "ath_date": data.get('ath_date'),
                    "atl": data.get('atl'),
                    "atl_change_percentage": data.get('atl_change_percentage'),
                    "atl_date": data.get('atl_date'),
                    "timestamp": current_time.isoformat(),
                    "last_updated": data.get('last_updated')
                }
                
                yield op.upsert("market_data", market_entry)
                log.info(f"Processed market data for {data.get('id')}")
        else:
            log.error(f"Failed to fetch market data for batch: {batch}")

    # Update state with last sync time
    yield op.checkpoint(state={"last_sync": current_time.isoformat()})

# Create connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    print("Running the Coingecko API connector...")
    connector.debug()
    print("Connector run complete.")