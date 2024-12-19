#!/bin/bash

# Find config.json by searching up through parent directories
CONFIG_PATH=$(pwd)
while [[ "$CONFIG_PATH" != "/" ]]; do
    if [[ -f "$CONFIG_PATH/config.json" ]]; then
        CONFIG_FILE="$CONFIG_PATH/config.json"
        break
    fi
    CONFIG_PATH=$(dirname "$CONFIG_PATH")
done

# Exit if config.json is not found
if [[ -z "$CONFIG_FILE" || ! -f "$CONFIG_FILE" ]]; then
    echo "Error: config.json not found in the directory hierarchy"
    exit 1
fi

# Prompt for the Fivetran Account Name
read -p "Enter your Fivetran Account Name [MDS_DATABRICKS_HOL]: " ACCOUNT_NAME
ACCOUNT_NAME=${ACCOUNT_NAME:-"MDS_DATABRICKS_HOL"}
echo "Preparing your Fivetran Account deployment configuration..."

# Read API key from config.json based on account name
API_KEY=$(jq -r ".fivetran.api_keys.$ACCOUNT_NAME" "$CONFIG_FILE")

if [ "$API_KEY" == "null" ]; then
    echo "Error: Account name not found in config.json"
    exit 1
fi

# Read NPS API key from nps_config.json
NPS_CONFIG_FILE="$(pwd)/files/nps_config.json"
if [[ ! -f "$NPS_CONFIG_FILE" ]]; then
    echo "Error: NPS configuration file not found at $NPS_CONFIG_FILE"
    exit 1
fi

# Create a temporary configuration JSON file
TEMP_CONFIG_FILE=$(mktemp)
NPS_API_KEY=$(jq -r '.apis.nps.api_key' "$NPS_CONFIG_FILE")

# Write a flat configuration with the API key
echo "{
    \"api_key\": \"[REDACTED]\"
}" > "$TEMP_CONFIG_FILE"

# Prompt for the Fivetran Destination Name
read -p "Enter your Fivetran Destination Name [ADLS_UNITY_CATALOG]: " DESTINATION_NAME
DESTINATION_NAME=${DESTINATION_NAME:-"ADLS_UNITY_CATALOG"}
echo "Preparing your Fivetran Destination deployment configuration..."

# Prompt for the Fivetran Connector Name
read -p "Enter a unique Fivetran Connector Name [default-connection]: " CONNECTION_NAME
CONNECTION_NAME=${CONNECTION_NAME:-"default-connection"}
echo "Preparing your Fivetran Connector deployment configuration..."

# Validate JSON
if ! jq empty "$TEMP_CONFIG_FILE" > /dev/null 2>&1; then
    echo "JSON validation failed"
    exit 1
fi

# Deploy with the temporary configuration file
fivetran deploy --api-key "$API_KEY" --destination "$DESTINATION_NAME" --connection "$CONNECTION_NAME" --configuration "$TEMP_CONFIG_FILE"

# Store the deployment result
DEPLOY_RESULT=$?

# Clean up the temporary configuration file
rm "$TEMP_CONFIG_FILE"

# Confirm deployment success
if [[ $DEPLOY_RESULT -eq 0 ]]; then
    echo "Fivetran Connector SDK deployment succeeded!"
else
    echo "Fivetran Connector SDK Deployment failed."
    exit 1
fi