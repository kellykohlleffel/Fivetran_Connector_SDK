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

### 📺 BestBuy TV Products (BestBuy Products API)  
[BestBuy Products API](https://developer.bestbuy.com/) custom connector that syncs detailed tv product data, including specifications, pricing, customer reviews, and product features.

### 📚 Books (OpenLibrary API)
[OpenLibrary API](https://openlibrary.org/developers/api) custom connector that syncs comprehensive book information including titles, authors, publication dates, ISBNs, and publisher details.

### 💰 Crypto Markets (CoinGecko API)
[CoinGecko API](https://www.coingecko.com/en/api) custom connector that syncs real-time cryptocurrency market data including prices, market caps, trading volumes, and historical metrics.

### 🛢️ Petroleum Production and Imports Data (EIA API)  
[EIA API](https://www.eia.gov/opendata/documentation.php) custom connector that retrieves comprehensive petroleum data including crude oil reserves, production statistics, and import information across various regions and time periods for the U.S. Energy Information Administration API.

### 💱 Exchange Rates (ExchangeRate API)  
[ExchangeRate API](https://www.exchangerate-api.com/docs/overview) custom connector that retrieves real-time exchange rates for USD paired with EUR, GBP, JPY, and AUD. Includes simulated historical data for a 7-day trend analysis without requiring a paid API subscription.

### 💊 FDA Drug Applications (OpenFDA API)
[OpenFDA API](https://open.fda.gov/apis/drug/drugsfda/) custom connector that syncs comprehensive drug application data including dosage forms, routes of administration, product details, and sponsor information across multiple administration routes.

### 🍳 Meals (TheMealDB API)
[TheMealDB API](https://www.themealdb.com/api.php) custom connector that retrieves detailed meal information including names, categories, cuisines, instructions, and ingredients.

### 🎬 Movies (TMDB API)
[The Movie Database API](https://developer.themoviedb.org/reference/configuration-details) custom connector focusing on Christopher Nolan's filmography, including movie details, cast credits, and reviews.

### 🏞️ National Parks (NPS API)
[National Park Service API](https://www.nps.gov/subjects/developer/index.htm) custom connector synchronizing U.S. National Parks information, including park details, fees, passes, and activities.

### 📰 NYT Most Popular Articles (NYT API)
[NYT Most Popular API](https://developer.nytimes.com/docs/most-popular-product/1/overview) custom connector that syncs the most viewed articles from the past 7 days, including article metadata, sections, keywords, and associated media. 

### ⭕ Oura Ring (Oura API)
[Oura API](https://cloud.ouraring.com/v2/docs) custom connector that syncs comprehensive health and wellness data including daily activity metrics, sleep patterns, and biometric measurements.

### 🐾 Pet Adoption (Petfinder Powered by Purina API)  
[Petfinder Powered by Purina API](https://www.petfinder.com/developers/v2/docs/) custom connector that retrieves comprehensive dog adoption information, including breed, physical characteristics, location, and adoption status.  

### 🌍 Solar System (Solar System OpenData API)
[Solar System OpenData API](https://api.le-systeme-solaire.net) custom connector providing celestial object data including names, types, orbital periods, and solar distances.

### 🚀 SpaceX (SpaceX API)
[SpaceX API](https://github.com/r-spacex/SpaceX-API/tree/master/docs) custom connector retrieving comprehensive SpaceX information about launches, rockets, and capsules.

### 🚘 Vehicle and Recall Data (NHTSA API)  
[NHTSA API](https://vpic.nhtsa.dot.gov/api/) custom connector that syncs detailed vehicle information, including make and model details, specifications, and recall notices.

### 💧 Water (USGS Water Services API)
[USGS Water Services API](https://waterservices.usgs.gov/docs/) custom connector syncing water data from Brazos River monitoring sites in Texas, including streamflow, gauge height, and temperature measurements.

### ⛅ Weather (National Weather Service API)
[National Weather Service API](https://www.weather.gov/documentation/services-web-api) custom connector retrieving weather forecast information for Cypress, TX, including temperature data and forecast periods. This example was based on the [Fivetran Connector SDK Quickstart Example](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/weather), which served as the original template for the additional connector examples in this repository.

## Repository Structure
```
examples/quick_start_examples/
└── bestbuy_products  # BestBuy Products API connector
└── books             # OpenLibrary API connector
└── crypto            # CoinGecko API connector
└── eia_petroleum     # US Energy Information Administration API connector
└── exchangerate      # ExchangeRate API connector
└── openFDA_drugs     # OpenFDA Drug Apps API connector
└── meals             # MealDB API connector
└── movies            # TMDB API connector
└── nationalparks     # NPS API connector
└── nytmostpopular    # New York Times (NYT) API connector
└── oura              # Oura Ring API connector
└── petfinder         # Petfinder powered by Purina API connector
└── solarsystem       # Solar System OpenData connector
└── spacex            # SpaceX API connector
└── vehicles          # NHTSA API connector
└── water             # USGS Water Services connector
└── weather           # National Weather Service connector
├── .gitattributes    # Git attributes configuration
├── .gitignore        # Git ignore rules
├── config.json       # Configuration file (local) for Fivetran Account API keys
├── README.md         # This documentation
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