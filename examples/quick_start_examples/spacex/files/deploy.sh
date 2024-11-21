#!/bin/bash

# Find config.json by searching up through parent directories
CONFIG_PATH=$(pwd)
while [[ "$CONFIG_PATH" != "/" ]]; do
    if [[ -f "$CONFIG_PATH/config.json" ]]; then
        break
    fi
    CONFIG_PATH=$(dirname "$CONFIG_PATH")
done

# Prompt for account name
read -p "Enter Fivetran Account Name: " ACCOUNT_NAME

# Read API key from config.json based on account name
API_KEY=$(jq -r ".fivetran.api_keys.$ACCOUNT_NAME" "$CONFIG_PATH/config.json")

if [ "$API_KEY" == "null" ]; then
    echo "Error: Account name not found in config.json"
    exit 1
fi

# Prompt for destination name
read -p "Enter destination name: " DESTINATION_NAME

# Prompt for connection name
read -p "Enter connection name: " CONNECTION_NAME

# Set defaults if empty
if [ -z "$DESTINATION_NAME" ]; then
    DESTINATION_NAME="MDS_DATABRICKS_HOL"
    echo "Using default destination name: $DESTINATION_NAME"
fi

if [ -z "$CONNECTION_NAME" ]; then
    CONNECTION_NAME="default-connection"
    echo "Using default connection name: $CONNECTION_NAME"
fi

fivetran deploy --api-key "$API_KEY" --destination "$DESTINATION_NAME" --connection "$CONNECTION_NAME"