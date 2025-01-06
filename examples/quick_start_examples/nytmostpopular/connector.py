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
            "table": "articles",
            "primary_key": ["id"],
            "columns": {}  # Let Fivetran infer data types
        },
        {
            "table": "media",
            "primary_key": ["media_id"],
            "columns": {}  # Let Fivetran infer data types
        }
    ]

def make_api_request(session, endpoint, params):
    """Make API request with error handling and logging"""
    try:
        base_url = "https://api.nytimes.com/svc/mostpopular/v2"
        full_url = f"{base_url}{endpoint}"
        
        log_params = params.copy()
        if 'api-key' in log_params:
            log_params['api-key'] = '***'
        
        Logging.warning(f"Making request to {endpoint} with params: {log_params}")
        response = session.get(full_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        return data
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed for {endpoint}: {str(e)}")
        if hasattr(response, 'status_code') and response.status_code == 429:
            Logging.warning("Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return make_api_request(session, endpoint, params)
        return {"results": []}

def update(configuration: dict, state: dict):
    """Retrieve data from the NYT Most Popular API."""
    session = create_retry_session()
    try:
        API_KEY = get_api_key(configuration)
        
        # Get most viewed articles for last 7 days
        Logging.warning("Starting NYT most viewed articles sync")
        params = {
            "api-key": API_KEY
        }
        
        articles_data = make_api_request(session, "/viewed/7.json", params)
        articles = articles_data.get("results", [])
        
        Logging.warning(f"Found {len(articles)} articles")
        
        media_id_counter = 1  # For generating unique media IDs
        
        for article in articles:
            # Process article
            article_data = {
                "id": article.get("id"),
                "url": article.get("url"),
                "title": article.get("title"),
                "abstract": article.get("abstract"),
                "published_date": article.get("published_date"),
                "updated_date": article.get("updated"),
                "section": article.get("section"),
                "subsection": article.get("subsection"),
                "byline": article.get("byline"),
                "type": article.get("type"),
                "adx_keywords": article.get("adx_keywords"),
                "views": article.get("views"),
                "des_facet": json.dumps(article.get("des_facet", [])),
                "org_facet": json.dumps(article.get("org_facet", [])),
                "per_facet": json.dumps(article.get("per_facet", [])),
                "geo_facet": json.dumps(article.get("geo_facet", []))
            }
            
            yield op.upsert(
                table="articles",
                data=article_data
            )
            
            # Process media
            for media_item in article.get("media", []):
                for metadata in media_item.get("media-metadata", []):
                    media_data = {
                        "media_id": f"{article.get('id')}_{media_id_counter}",
                        "article_id": article.get("id"),
                        "article_title": article.get("title"),  # Adding article title for easier joins/queries
                        "type": media_item.get("type"),
                        "subtype": media_item.get("subtype"),
                        "caption": media_item.get("caption"),
                        "copyright": media_item.get("copyright"),
                        "url": metadata.get("url"),
                        "format": metadata.get("format"),
                        "height": metadata.get("height"),
                        "width": metadata.get("width")
                    }
                    
                    yield op.upsert(
                        table="media",
                        data=media_data
                    )
                    media_id_counter += 1
            
            time.sleep(0.25)  # Rate limiting
        
        yield op.checkpoint(state={})
        
    except Exception as e:
        Logging.warning(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting NYT connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")