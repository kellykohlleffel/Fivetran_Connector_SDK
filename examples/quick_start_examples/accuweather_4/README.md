# Fivetran Custom Connector: AccuWeather Forecast API

## Overview
This custom connector retrieves 5-day weather forecast data from the AccuWeather API. It's designed to work within the free tier limitations (50 API calls per day) and implements appropriate rate limiting and error handling.

## Features
- Retrieves 5-day weather forecasts for configured locations
- Handles AccuWeather API rate limits (50 calls/day)
- Implements retry logic with exponential backoff
- Supports multiple location tracking
- Provides comprehensive weather data including:
  - Temperature ranges
  - Precipitation probabilities
  - Wind conditions
  - Weather icons and descriptions
  - Day and night forecasts

## Configuration
1. Obtain an AccuWeather API key from [developer.accuweather.com](https://developer.accuweather.com)
2. Add your API key to configuration.json
3. Modify the locations list in connector.py if needed

## Rate Limiting
- Free tier: 50 API calls per day
- Connector implements appropriate delays between requests
- Exponential backoff for rate limit handling

## Tables
### daily_forecasts
Contains detailed weather forecast data:
- Temperature ranges
- Precipitation probabilities
- Wind conditions
- Weather descriptions
- Separate day/night forecasts

## Usage
1. Install dependencies: