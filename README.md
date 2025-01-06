# Fivetran Connector SDK Examples

## Overview
This repository contains a collection of example custom connectors using [Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk), demonstrating how to build custom data connectors in Python. Each example showcases different API integrations and data synchronization patterns, providing practical templates for building your own custom connectors.

The Fivetran Connector SDK enables you to:
- Code API interactions in Python
- Deploy connectors as Fivetran extensions
- Leverage Fivetran's automated management of:
  - Scheduled runs
  - Compute resources
  - Orchestration
  - Scaling
  - Resyncs
  - Log management
  - Comprehensive data writing
  - Retries
  - Schema inference
  - Security
  - Idempotency

## Quick Start Examples

### 📚 Books (OpenLibrary API)
[OpenLibrary API](https://openlibrary.org/developers/api) integration that syncs comprehensive book information including titles, authors, publication dates, ISBNs, and publisher details.

### 💰 Crypto Markets (CoinGecko API)
[CoinGecko API](https://www.coingecko.com/en/api) integration that syncs real-time cryptocurrency market data including prices, market caps, trading volumes, and historical metrics.

### 🍳 Meals (TheMealDB API)
[TheMealDB API](https://www.themealdb.com/api.php) connector that retrieves detailed meal information including names, categories, cuisines, instructions, and ingredients.

### 🎬 Movies (TMDB API)
[The Movie Database API](https://developer.themoviedb.org/reference/configuration-details) implementation focusing on Christopher Nolan's filmography, including movie details, cast credits, and reviews.

### 🏞️ National Parks (NPS API)
[National Park Service API](https://www.nps.gov/subjects/developer/index.htm) connector synchronizing U.S. National Parks information, including park details, fees, passes, and activities.

### 📰 NYT Most Popular Articles (NYT API)
[NYT Most Popular API](https://developer.nytimes.com/docs/most-popular-product/1/overview) integration that syncs the most viewed articles from the past 7 days, including article metadata, sections, keywords, and associated media. 

### 🌍 Solar System (Solar System OpenData API)
[Solar System OpenData API](https://api.le-systeme-solaire.net) integration providing celestial object data including names, types, orbital periods, and solar distances.

### 🚀 SpaceX (SpaceX API)
[SpaceX API](https://github.com/r-spacex/SpaceX-API/tree/master/docs) connector retrieving comprehensive SpaceX information about launches, rockets, and capsules.

### 💧 Water (USGS Water Services API)
[USGS Water Services API](https://waterservices.usgs.gov/docs/) implementation syncing water data from Brazos River monitoring sites in Texas, including streamflow, gauge height, and temperature measurements.

### ⛅ Weather (National Weather Service API)
[National Weather Service API](https://www.weather.gov/documentation/services-web-api) connector retrieving weather forecast information for Cypress, TX, including temperature data and forecast periods. This example was based on the [Fivetran Connector SDK Quickstart Example](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/weather), which served as the original template for the additional connector examples in this repository.

## Repository Structure
```
examples/quick_start_examples/
└── books           # OpenLibrary API connector
└── crypto          # CoinGecko API connector
└── meals           # MealDB API connector
└── movies          # TMDB API connector
└── nationalparks   # NPS API connector
└── nytmostpopular  # New York Times (NYT) API connector
└── solarsystem     # Solar System OpenData connector
└── spacex          # SpaceX API connector
└── water           # USGS Water Services connector
└── weather         # National Weather Service connector
├── .gitattributes  # Git attributes configuration
├── .gitignore      # Git ignore rules
├── README.md       # This documentation
```

## Getting Started

Each example connector in the `quick_start_examples` directory contains:
- Detailed README with setup and usage instructions
- Complete connector implementation
- Deployment scripts
- Configuration templates
- Example outputs and visualizations

For specific implementation details and setup instructions, refer to the README in each connector's directory.

## Documentation
- [Fivetran Connector SDK Technical Reference](https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)

## Contributing
To add a new connector example:
1. Create a new directory under `examples/quick_start_examples/`
2. Include a comprehensive README following the existing pattern
3. Implement the connector and deployment scripts
4. Add the connector to this main README under "Quick Start Examples"

## Support
For Fivetran Connector SDK support:
- Consult the [Fivetran Documentation](https://fivetran.com/docs/connectors/connector-sdk)