# Fivetran_Connector_SDK: Solar System OpenData API

## Overview
This Fivetran custom connector leverages the Fivetran Connector SDK to retrieve data from the [Solar System OpenData API](https://api.le-systeme-solaire.net), enabling syncing of comprehensive celestial object information including names, types, orbital periods, and distances from the Sun.

Fivetran's Connector SDK enables you to use Python to code the interaction with the Solar System OpenData API data source. This example shows the use of a connector.py file that calls Solar System OpenData API. From there, the connector is deployed as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources, orchestration, scaling, resyncs, and log management. In addition, Fivetran handles comprehensive writing to the destination of your choice managing retries, schema inference, security, and idempotency.

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

![Solar System Sync Status](images/fivetran_syncstatus_solarsystem_connector_sdk.png)

## Attribution
<img src="https://api.le-systeme-solaire.net/assets/images/logo.png" alt="Le Système Solaire Logo" width="50"/>

This custom connector uses data from api.le-systeme-solaire.net.

For more information, please visit:
[Le Système Solaire API](https://api.le-systeme-solaire.net/en/)

## Features
- Retrieves comprehensive celestial object data from Solar System OpenData API
- Processes detailed information including orbital periods and distances
- Handles multiple object types (planets, moons, asteroids)
- Supports debug mode for local testing
- Provides detailed logging for troubleshooting
- No authentication required

## API Interaction
The connector establishes interaction with Solar System OpenData API through several key components:

### Core Functions

#### API Request Implementation
```python
response = rq.get("https://api.le-systeme-solaire.net/rest/bodies/")
```
- Uses simple GET request to bodies endpoint
- Returns JSON response with celestial object records
- No authentication required
- Default timeout handling via requests library
- Native error handling for HTTP responses

#### Data Processing Functions
- Extracts celestial object details from JSON response
- Handles missing fields with default values:
  ```python
  object_id = obj.get("id", "Unknown ID")
  name = obj.get("englishName", "Unknown Name")
  type_ = obj.get("bodyType", "Unknown Type")
  ```
- Processes orbital data and distances
- Provides debug logging of processed records

#### Error Handling
- Handles missing orbital data fields
- Validates object IDs
- Manages empty responses
- Logs processing details for debugging

### Data Retrieval Strategy

#### Celestial Object Collection
- Uses bodies endpoint to retrieve all objects
- Processes multiple object types
- Handles astronomical measurements

#### Response Processing 
- Field validation and extraction
- Default value handling for missing data
- Measurement standardization
- Data transformation for Fivetran schema

### Security Features
- No API key required
- Safe handling of configuration data
- Protected credential management through Fivetran's infrastructure
- Secure logging practices

## Directory Structure
```
solarsystem/
├── __pycache__/        # Python bytecode cache directory
├── files/              # Generated directory for Fivetran files
│   ├── state.json     # State tracking for incremental syncs
│   └── warehouse.db   # Local testing database
├── images/            # Documentation images
├── connector.py       # Main connector implementation
├── debug.sh          # Debug deployment script
├── deploy.sh         # Production deployment script
├── README.md         # Project documentation
└── requirements.txt   # Python dependencies
```

## File Details

### connector.py
Main connector implementation file that handles:
- API requests and response processing
- Data transformation and schema definition
- Astronomical data processing
- Error handling and logging

### deploy.sh
```bash
#!/bin/bash

# Find config.json by searching up through parent directories
CONFIG_PATH=$(pwd)
while [[ "$CONFIG_PATH" != "/" ]]; do
    if [[ -f "$CONFIG_PATH/config.json" ]]; then
        break
    fi
    CONFIG_PATH=$(dirname "$CONFIG_PATH")
done

# Prompt for the Fivetran Account Name
read -p "Enter your Fivetran Account Name [MDS_DATABRICKS_HOL]: " ACCOUNT_NAME
ACCOUNT_NAME=${ACCOUNT_NAME:-"MDS_DATABRICKS_HOL"}

# Read API key from config.json based on account name
API_KEY=$(jq -r ".fivetran.api_keys.$ACCOUNT_NAME" "$CONFIG_PATH/config.json")

if [ "$API_KEY" == "null" ]; then
    echo "Error: Account name not found in config.json"
    exit 1
fi

# Prompt for the Fivetran Destination Name
read -p "Enter your Fivetran Destination Name [ADLS_UNITY_CATALOG]: " DESTINATION_NAME
DESTINATION_NAME=${DESTINATION_NAME:-"ADLS_UNITY_CATALOG"}

# Prompt for the Fivetran Connector Name
read -p "Enter a unique Fivetran Connector Name [default-connection]: " CONNECTION_NAME
CONNECTION_NAME=${CONNECTION_NAME:-"default-connection"}

fivetran deploy --api-key "$API_KEY" --destination "$DESTINATION_NAME" --connection "$CONNECTION_NAME"
```

### debug.sh
```bash
#!/bin/bash
echo "Starting debug process..."

echo "Running fivetran reset..."
fivetran reset

echo "Creating files directory..."
mkdir -p files

echo "Contents of files directory:"
ls -la files/

echo "Running fivetran debug..."
fivetran debug
```

### images/
Contains documentation screenshots and images:
- Directory structure screenshots
- Sample output images
- Configuration examples
- Other visual documentation

## Setup Instructions

### Prerequisites
* Python 3.8+
* Fivetran Connector SDK
* Fivetran Account with at least one Fivetran destination setup

### Installation Steps
1. Create the project directory structure:
```bash
mkdir -p solarsystem
cd solarsystem
```

2. Create a Python virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install the Fivetran Connector SDK:
```bash
pip install fivetran-connector-sdk
```

4. Create the necessary files:
```bash
touch connector.py debug.sh deploy.sh
chmod +x debug.sh deploy.sh
```

5. Set up .gitignore:
```bash
touch .gitignore
echo "files/
__pycache__/
*.pyc
.DS_Store" > .gitignore
```

## Usage

### Local Testing
1. Ensure your virtual environment is activated
2. Run the debug script:
```bash
chmod +x debug.sh
./debug.sh
```

The debug process will:
1. Reset any existing state
2. Create the files directory
3. Retrieve celestial object data
4. Log the process details
5. Create local database files for testing

### Production Deployment
Execute the deployment script:
```bash
chmod +x deploy.sh
./deploy.sh
```

The script will:
* Find and read your Fivetran configuration
* Prompt for account details and deployment options
* Deploy the connector to your Fivetran destination

### Expected Output
The connector will:
1. Display process status
2. Show number of celestial objects retrieved
3. Print a formatted table of objects:
   - ID
   - Name
   - Type
   - Orbital Period
   - Distance from Sun
4. Log sync statistics

## Data Tables

### solar_system_object
Primary table containing celestial object information:
* id (STRING, Primary Key)
* name (STRING)
* type (STRING)
* orbital_period (FLOAT)
* distance_from_sun (FLOAT)

## Troubleshooting

### Common Issues
1. API Response Issues:
```
Error: No objects found in response
```
* Verify Solar System OpenData API is accessible
* Check network connectivity

2. Directory Structure:
```
No such file or directory: 'files/warehouse.db'
```
* Ensure debug.sh has created the files directory
* Check file permissions

3. Python Environment:
```
ModuleNotFoundError: No module named 'fivetran_connector_sdk'
```
* Verify virtual environment is activated
* Reinstall SDK if necessary

## Security Notes
* Use .gitignore to prevent accidental commits of sensitive files
* Keep your virtual environment isolated from other projects
* Follow Fivetran's security best practices for deployment

## Development Notes
* Make code changes in connector.py
* Test changes using debug.sh
* Monitor logs for any issues
* Use the Fivetran SDK documentation for reference

## Support
For issues or questions:
1. Check the [Solar System OpenData API Documentation](https://api.le-systeme-solaire.net/swagger/)
2. Review the [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
3. Contact your organization's Fivetran administrator

## Using the new Solar System dataset

### Snowflake Snowsight:

![Snowflake Snowsight Data Preview Screenshot](./images/snowflake_snowsight_datapreview_solarsystem_connector_sdk.png)

### Snowflake Snowsight Dashboard with the new SolarSystem data

![Snowflake Snowsight Dashboard Screenshot](./images/snowflake_snowsight_dashboard_solarsystem_connector_sdk.png)

### SQL queries for SolarSystem data (update the database and schema names in your Snowsight worksheet)

#### Count of Celestial Objects by Type
```
SELECT 
    type AS Object_Type,
    COUNT(id) AS Object_Count
FROM 
    HOL_DATABASE.SOLARSYSTEM_CONNECTOR_SDK.solar_system_object
GROUP BY 
    type
ORDER BY 
    Object_Count DESC;
```
#### Celestial Object Furthest Distance from the Sun
```
SELECT 
    name AS Object_Name,
    distance_from_sun AS Distance_from_Sun_km
FROM 
    HOL_DATABASE.SOLARSYSTEM_CONNECTOR_SDK.solar_system_object
WHERE 
    distance_from_sun IS NOT NULL
ORDER BY 
    Distance_from_Sun_km DESC
LIMIT 1;
```
#### Average Orbital Period by Object Type
```
SELECT 
    type AS Object_Type,
    ROUND(AVG(orbital_period)) AS Average_Orbital_Period_days
FROM 
    HOL_DATABASE.SOLARSYSTEM_CONNECTOR_SDK.solar_system_object
WHERE 
    orbital_period IS NOT NULL
GROUP BY 
    type
ORDER BY 
    Average_Orbital_Period_days DESC;
```
