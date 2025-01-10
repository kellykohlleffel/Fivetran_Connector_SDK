import json
import os
import time
from datetime import datetime
import requests as rq
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op

def create_retry_session():
    """Create a requests session with retry logic."""
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
            raise KeyError("Missing API key in configuration.")
        return str(api_key)
    except Exception as e:
        raise KeyError(f"Error retrieving API key: {str(e)}")

def schema(configuration: dict):
    """Define the table schema for Fivetran."""
    return [
        {
            "table": "tv_products",
            "primary_key": ["sku"],
            "columns": {}  # Let Fivetran infer data types
        }
    ]

def make_api_request(session, api_key, page=1, page_size=100):
    """Make API request with error handling and logging."""
    try:
        base_url = "https://api.bestbuy.com/v1/products"

        # Query parameters for TV products using categoryPath.name
        params = {
            "apiKey": api_key,
            "format": "json",
            "show": "sku,name,manufacturer,modelNumber,regularPrice,salePrice,onSale,"
                    "customerReviewAverage,customerReviewCount,description,features.feature,"
                    "color,depth,height,weight,width,condition,digital,hdmiInputs,screenSizeIn",
            "pageSize": page_size,
            "page": page,
            "sort": "customerReviewCount.dsc"
        }

        # Using categoryPath.name to filter for TVs
        query = '(categoryPath.name="All Flat-Screen TVs")'

        Logging.warning(f"Making request to BestBuy API with category filter: 'All Flat-Screen TVs', Page {page}")
        response = session.get(f"{base_url}({query})", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        return data
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed: {str(e)}")
        if hasattr(response, 'status_code') and response.status_code == 429:
            Logging.warning("Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return make_api_request(session, api_key, page, page_size)
        return {"products": [], "totalPages": 0}

def update(configuration: dict, state: dict):
    """Retrieve TV product data from the BestBuy API."""
    session = create_retry_session()
    try:
        api_key = get_api_key(configuration)

        Logging.warning("Starting sync for TV products")
        total_products_processed = 0

        # Get products with pagination
        page = 1
        page_size = 100  # Maximum allowed by API
        max_pages = 5  # Limit to 5 pages to manage API calls

        while page <= max_pages:
            data = make_api_request(session, api_key, page, page_size)
            products = data.get("products", [])
            total_pages = data.get("totalPages", 0)

            if not products:
                break

            Logging.warning(f"Processing {len(products)} TV products from page {page}")

            for product in products:
                # Extract features as a JSON string
                features = json.dumps(product.get("features", []))

                # Process product data
                product_data = {
                    "sku": product.get("sku"),
                    "name": product.get("name"),
                    "manufacturer": product.get("manufacturer"),
                    "model_number": product.get("modelNumber"),
                    "regular_price": product.get("regularPrice"),
                    "sale_price": product.get("salePrice"),
                    "on_sale": product.get("onSale", False),
                    "customer_review_average": product.get("customerReviewAverage"),
                    "customer_review_count": product.get("customerReviewCount"),
                    "description": product.get("description"),
                    "features": features,
                    "color": product.get("color"),
                    "depth": product.get("depth"),
                    "height": product.get("height"),
                    "weight": product.get("weight"),
                    "width": product.get("width"),
                    "condition": product.get("condition"),
                    "digital": product.get("digital", False),
                    "hdmi_inputs": product.get("hdmiInputs"),
                    "screen_size": product.get("screenSizeIn"),
                    "last_updated": datetime.utcnow().isoformat()
                }

                yield op.upsert(
                    table="tv_products",
                    data=product_data
                )

            total_products_processed += len(products)
            Logging.warning(f"Total TV products processed so far: {total_products_processed}")

            if page >= total_pages or page >= max_pages:
                break

            page += 1
            time.sleep(1)  # Rate limiting between pages

        Logging.warning(f"Sync complete. Processed {total_products_processed} TV products.")
        yield op.checkpoint(state={})

    except Exception as e:
        Logging.warning(f"Error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting BestBuy TV products connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")
