# Fivetran_Connector_SDK: National Parks Service API

## Overview
This Fivetran custom connector is a simple example that uses the Fivetran Connector SDK to retrieve data from the [National Park Service (NPS) API](https://www.nps.gov/subjects/developer/index.htm), allowing you to sync comprehensive information about U.S. National Parks, including parks details, fees and passes, and people associated with these parks. 

Fivetran's Connector SDK enables you to use Python to code the interaction with the NPS API data source. This example shows the use of a connector.py file that calls NPS API. From there, the connector is deployed as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources, orchestration, scaling, resyncs, and log management. In addition, Fivetran handles comprehensive writing to the destination of your choice managing retries, schema inference, security, and idempotency.

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

## Features
- Retrieve details about U.S. National Parks
- Sync information about park fees and passes
- Capture biographical information about people associated with national parks
- Customizable data retrieval using Fivetran Connector SDK

## Connector Architecture and Functionality

### Key Technical Components

#### API Interaction
The connector utilizes the `requests` library to interact with the National Park Service API, implementing robust error handling and retry mechanisms. Key functions include:

- `create_retry_session()`: Configures HTTP request sessions with built-in retry logic for handling potential network issues
- `make_api_request()`: Manages API calls with error handling, rate limit protection, and logging
- Handles API pagination and batch processing of data

#### Data Synchronization Process
The `update()` function orchestrates the entire data sync process with three primary stages:

1. **Parks Sync**
   - Filters for only National Parks
   - Extracts detailed park information
   - Captures park-specific metadata like:
     - Geographical coordinates
     - Park activities
     - Designation details

2. **People Sync**
   - Retrieves people associated with National Parks
   - Filters and processes biographical information
   - Creates relationships between people and specific parks

3. **Fees and Passes Sync**
   - Extracts entrance fees and passes for each National Park
   - Captures pricing, description, and validity information

#### Schema Definition
The `schema()` function defines the structure for three primary tables:
- `parks`: Comprehensive park information
- `people`: Details about individuals associated with parks
- `feespasses`: Pricing and pass information

#### Error Handling and Logging
- Implements comprehensive error catching
- Uses Fivetran's `Logging` module for detailed tracking
- Gracefully handles API request failures
- Provides checkpointing to resume interrupted syncs

### Advanced Features

#### Configuration Flexibility
- Supports dynamic API key retrieval
- Allows filtering and customization of data extraction
- Handles nested configuration parsing

#### Performance Optimization
- Batch processing of API responses
- Efficient data transformation
- Minimal memory footprint through generator-based sync

### Data Transformation Techniques
- Converts complex API responses into structured tabular data
- Handles missing or incomplete data gracefully
- Converts geographical and numerical data types
- Uses JSON serialization for complex fields like activities and related parks

## Technical Design Principles
- Modular function design
- Separation of concerns
- Robust error handling
- Adherence to Fivetran Connector SDK best practices

## Prerequisites
- Python 3.8+
- Fivetran Connector SDK
- [NPS API Key](https://www.nps.gov/subjects/developer/get-started.htm)
- Fivetran Account

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/nationalparks-fivetran-connector.git
cd nationalparks-fivetran-connector
```

### 2. Create Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## Configuration

### API Key Setup
1. Obtain an NPS API Key from the [National Park Service Developer Portal](https://www.nps.gov/subjects/developer/get-started.htm)
2. Create a configuration file `files/nps_config.json`:
```json
{
    "apis": {
        "nps": {
            "api_key": "YOUR_NPS_API_KEY"
        }
    }
}
```

### Fivetran Account Configuration
1. Locate the main `config.json` with Fivetran account details
2. Ensure your Fivetran account API key is included for the specific account you'll be using

## Development and Testing

### Running Connector in Debug Mode
To test the connector locally and verify data retrieval:
```bash
fivetran debug
```

### Deployment
To deploy the connector to Fivetran:
```bash
./files/deploy.sh
```

During deployment, you'll be prompted to:
- Select Fivetran Account Name
- Specify Destination Name
- Choose a Unique Connector Name

## Data Tables

### parks
- **Columns**: 
  - `park_id`: Unique identifier for the park
  - `name`: Full name of the park
  - `description`: Park description
  - `state`: State(s) where the park is located
  - `latitude`: Geographical latitude
  - `longitude`: Geographical longitude
  - `activities`: List of available activities
  - `designation`: Park designation type
- **Primary Key**: `park_id`

### feespasses
- **Columns**:
  - `pass_id`: Unique identifier for the pass
  - `park_id`: Related park identifier
  - `park_name`: Name of the park
  - `title`: Pass or fee title
  - `cost`: Price of the pass or fee
  - `description`: Pass or fee description
  - `valid_for`: Validity period or usage
- **Primary Key**: `pass_id`

### people
- **Columns**:
  - `person_id`: Unique identifier for the person
  - `name`: Person's name
  - `title`: Person's title or role
  - `description`: Description of the person
  - `url`: Additional information URL
  - `related_parks`: List of related park codes
  - `park_names`: Names of related parks
- **Primary Key**: `person_id`

## Troubleshooting

### Common Issues
- Verify NPS API Key is valid and active
- Check network connectivity
- Ensure all dependencies are installed
- Confirm Fivetran account permissions

### Logging
- Check Fivetran dashboard for sync logs
- Review local debug output for detailed information

## Performance Considerations
- The connector retrieves data in batches
- Initial sync may take longer depending on the number of parks and associated records

## Security
- Store API keys securely
- Do not commit sensitive configuration files to version control
- Use environment variables or secure secret management

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
[Specify your license here]

## Support
For issues or questions, please open a GitHub issue or contact your organization's Fivetran administrator.

## Version
1.0.0

---

### Example Workflow
```bash
# Clone the repository
git clone https://github.com/your-org/nationalparks-fivetran-connector.git

# Navigate to project directory
cd nationalparks-fivetran-connector

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Debug the connector
fivetran debug

# Deploy to Fivetran
./files/deploy.sh
```