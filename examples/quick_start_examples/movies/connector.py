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
            "table": "movies",
            "primary_key": ["movie_id"],
            "columns": {
                "movie_id": "STRING",
                "title": "STRING",
                "overview": "STRING",
                "release_date": "STRING",
                "runtime": "STRING",
                "budget": "STRING",
                "revenue": "STRING",
                "vote_average": "STRING",
                "vote_count": "STRING",
                "popularity": "STRING"
            }
        },
        {
            "table": "credits",
            "primary_key": ["credit_id"],
            "columns": {
                "credit_id": "STRING",
                "movie_id": "STRING",
                "title": "STRING",
                "person_id": "STRING",
                "person_name": "STRING",
                "character": "STRING",
                "order": "STRING",
                "cast_id": "STRING"
            }
        },
        {
            "table": "reviews",
            "primary_key": ["review_id"],
            "columns": {
                "review_id": "STRING",
                "movie_id": "STRING",
                "title": "STRING",
                "author": "STRING",
                "content": "STRING",
                "created_at": "STRING",
                "updated_at": "STRING",
                "rating": "STRING"
            }
        }
    ]

def make_api_request(session, endpoint, params):
    """Make API request with error handling and logging"""
    try:
        base_url = "https://api.themoviedb.org/3"
        full_url = f"{base_url}{endpoint}"
        
        log_params = params.copy()
        if 'api_key' in log_params:
            log_params['api_key'] = '***'
        
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
        return {"results": [], "total_results": 0}

def get_nolan_movies(session, api_key):
    """Get all Christopher Nolan movies"""
    # Christopher Nolan's TMDB ID is 525
    params = {
        "api_key": api_key,
        "language": "en-US"
    }
    
    return make_api_request(session, "/person/525/movie_credits", params)

def update(configuration: dict, state: dict):
    """Retrieve data from the TMDB API."""
    session = create_retry_session()
    try:
        API_KEY = get_api_key(configuration)
        
        # Get all Nolan movies
        Logging.warning("Starting Christopher Nolan movies sync")
        nolan_credits = get_nolan_movies(session, API_KEY).get("crew", [])
        
        # Filter for movies where Nolan is director
        nolan_movies = [movie for movie in nolan_credits if movie.get("job") == "Director"]
        Logging.warning(f"Found {len(nolan_movies)} movies directed by Christopher Nolan")
        
        for movie in nolan_movies:
            movie_id = movie.get("id")
            if not movie_id:
                continue
                
            # Get detailed movie information
            movie_params = {
                "api_key": API_KEY,
                "language": "en-US",
                "append_to_response": "credits,reviews"
            }
            
            movie_details = make_api_request(session, f"/movie/{movie_id}", movie_params)
            
            # Insert movie details
            yield op.upsert(
                table="movies",
                data={
                    "movie_id": str(movie_id),  # Convert to string
                    "title": movie_details.get("title"),
                    "overview": movie_details.get("overview"),
                    "release_date": movie_details.get("release_date"),
                    "runtime": movie_details.get("runtime"),
                    "budget": movie_details.get("budget"),
                    "revenue": movie_details.get("revenue"),
                    "vote_average": float(movie_details.get("vote_average")) if movie_details.get("vote_average") else None,
                    "vote_count": movie_details.get("vote_count"),
                    "popularity": float(movie_details.get("popularity")) if movie_details.get("popularity") else None
                }
            )
            
            # Process cast only (no crew)
            for cast_member in movie_details.get("credits", {}).get("cast", []):
                yield op.upsert(
                    table="credits",
                    data={
                        "credit_id": cast_member.get("credit_id"),
                        "movie_id": str(movie_id),  # Convert to string
                        "title": movie_details.get("title"),
                        "person_id": str(cast_member.get("id")),  # Convert to string
                        "person_name": cast_member.get("name"),
                        "character": cast_member.get("character", ""),
                        "order": cast_member.get("order"),
                        "cast_id": cast_member.get("cast_id")
                    }
                )
            
            # Process reviews
            for review in movie_details.get("reviews", {}).get("results", []):
                yield op.upsert(
                    table="reviews",
                    data={
                        "review_id": str(review.get("id")),  # Convert to string
                        "movie_id": str(movie_id),  # Convert to string
                        "title": movie_details.get("title"),
                        "author": review.get("author"),
                        "content": review.get("content"),
                        "created_at": review.get("created_at"),
                        "updated_at": review.get("updated_at"),
                        "rating": float(review.get("author_details", {}).get("rating")) if review.get("author_details", {}).get("rating") else None
                    }
                )
            
            time.sleep(0.25)  # Rate limiting
        
        yield op.checkpoint(state={})
        
    except Exception as e:
        Logging.warning(f"Major error during sync: {str(e)}")
        raise

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting TMDB connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")