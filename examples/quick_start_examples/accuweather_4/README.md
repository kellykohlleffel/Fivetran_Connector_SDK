# Fivetran Connector SDK: AccuWeather 5 Day Forecast

## Overview
This Fivetran custom connector integrates with the AccuWeather API to retrieve 5-day weather forecast data. The connector enables automated syncing of weather forecasts, including temperature, precipitation, wind conditions, and other meteorological data for specified locations.

## Attribution
<img src="https://developer.accuweather.com/sites/default/files/AccuWeather_Logo_Dark_Orange.svg" alt="AccuWeather Logo" width="200"/>

This connector uses the AccuWeather API. Weather forecasts and data provided by AccuWeather.

For more information about the AccuWeather API, visit:
[AccuWeather API Documentation](https://developer.accuweather.com/apis)

## Features
- Retrieves 5-day weather forecasts using the AccuWeather API
- Includes daily forecasts with temperature ranges, precipitation probability, and conditions
- Captures detailed weather metrics including:
  - Temperature (min/max)
  - Precipitation type and probability
  - Wind speed and direction
  - Cloud cover and UV index
  - Day/night conditions
- Implements robust error handling and retry mechanisms
- Supports location-based forecasts using AccuWeather location keys
- Provides detailed logging for troubleshooting
- Follows Fivetran Connector SDK best practices

## API Interaction

### Authentication
The connector uses an API key for authentication:
```python
headers = {
    'apikey': configuration['api_key']
}
```

### Rate Limiting
- Free Tier: 50 calls/day
- Paid Tiers: Various limits based on subscription
- Implements exponential backoff for rate limit handling

### Endpoints Used
- `/locations/v1/cities/search` - Location search
- `/forecasts/v1/daily/5day/{locationKey}` - 5-day forecast

## Core Functions

### create_retry_session()
```python
def create_retry_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
```

### make_api_request()
```python
def make_api_request(url, headers):
    session = create_retry_session()
    try:
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"API request failed: {str(e)}")
        raise
```

## Directory Structure
```
accuweather/
├── files/
│   ├── spec.json
│   ├── state.json
│   └── configuration.json
├── connector.py
├── configuration.json
├── spec.json
├── requirements.txt
└── README.md
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- Fivetran Connector SDK
- AccuWeather API Key (obtain from [AccuWeather API Portal](https://developer.accuweather.com/))

### Installation
1. Create virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install fivetran-connector-sdk requests
```

3. Configure API key:
```json
{
    "api_key": "YOUR_ACCUWEATHER_API_KEY",
    "location_key": "347625"  // Optional: Default location key
}
```

## Data Tables

### forecast_daily
```sql
CREATE TABLE forecast_daily (
    location_key STRING,
    forecast_date STRING,
    min_temp FLOAT,
    max_temp FLOAT,
    day_condition STRING,
    night_condition STRING,
    precipitation_probability INT,
    precipitation_type STRING,
    wind_speed FLOAT,
    wind_direction STRING,
    cloud_cover INT,
    uv_index INT,
    PRIMARY KEY (location_key, forecast_date)
);
```

## Troubleshooting

### Common Issues
1. Rate Limit Exceeded:
```
Error: API request failed with status 429
```
- Wait for rate limit reset or upgrade API tier

2. Invalid API Key:
```
Error: API request failed with status 401
```
- Verify API key in configuration.json

## Security Notes
- Store API key securely in configuration.json
- Never commit API keys to version control
- Use .gitignore to exclude sensitive files

## Development Notes
- Test changes using `fivetran debug`
- Monitor API usage to stay within limits
- Implement proper error handling
- Use logging for debugging

## Support
For assistance:
1. Check [AccuWeather API Documentation](https://developer.accuweather.com/apis)
2. Review [Fivetran SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
3. Contact AccuWeather API support for API-specific issues

## Bonus Section: Location Management
Add multiple locations using location keys:
```json
{
    "api_key": "YOUR_API_KEY",
    "locations": [
        "347625",  // New York
        "348308",  // Los Angeles
        "349727"   // Chicago
    ]
}
```

## Using the Dataset
Example query for average temperatures:
```sql
SELECT 
    location_key,
    AVG(min_temp) as avg_min_temp,
    AVG(max_temp) as avg_max_temp
FROM forecast_daily
GROUP BY location_key;
```

## Streamlit in Snowflake Example Application
```python
import streamlit as st
from snowflake.snowpark.context import get_active_session

def main():
    session = get_active_session()

    st.title("Weather Forecast Dashboard")

    # Get available locations
    locations = session.sql("""
        SELECT DISTINCT location_key 
        FROM forecast_daily
        ORDER BY location_key
    """).collect()

    # Location selector
    selected_location = st.selectbox(
        "Select Location",
        [row[0] for row in locations]
    )

    # Display forecast
    if selected_location:
        forecast = session.sql(f"""
            SELECT 
                forecast_date,
                min_temp,
                max_temp,
                day_condition,
                precipitation_probability
            FROM forecast_daily
            WHERE location_key = '{selected_location}'
            ORDER BY forecast_date
        """).collect()

        # Create forecast display
        for row in forecast:
            st.write(f"### {row[0]}")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Temperature", f"{row[2]}°F", f"Low: {row[1]}°F")
            with col2:
                st.write(f"Condition: {row[3]}")
            with col3:
                st.write(f"Precipitation: {row[4]}%")

if __name__ == "__main__":
    main()
```

## Upgrading to Paid API Tier
To access additional features and higher rate limits:
1. Visit [AccuWeather API Portal](https://developer.accuweather.com/packages)
2. Choose a subscription package
3. Update your API key in configuration.json
4. Adjust rate limiting parameters in connector.py

## API Pricing
| Tier | Calls/Day | Price/Month | Features |
|------|-----------|-------------|-----------|
| Free | 50 | $0 | Basic forecast |
| Basic | 225,000 | $25 | + Hourly forecast |
| Standard | 2,000,000 | $180 | + Severe weather |
| Premium | Custom | Contact sales | Enterprise features |

Note: Pricing subject to change. Check AccuWeather website for current rates.