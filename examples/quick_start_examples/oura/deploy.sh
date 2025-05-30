#!/bin/bash

# Colors and formatting
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Display banner
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo -e "${BLUE}${BOLD}            Fivetran Connector Deployment Script          ${NC}"
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo

# Absolute path to root-level config.json
ROOT_CONFIG_PATH="$HOME/Documents/GitHub/Fivetran_Connector_SDK/config.json"

# Validate the root config.json file exists
if [[ ! -f "$ROOT_CONFIG_PATH" ]]; then
    echo -e "${YELLOW}Error: Root config.json not found at expected path: $ROOT_CONFIG_PATH${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Found root config.json at: $ROOT_CONFIG_PATH${NC}"

# Validate the local configuration.json file exists
if [[ ! -f "configuration.json" ]]; then
    echo -e "${YELLOW}Error: Local configuration.json not found in this directory!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Found local configuration.json${NC}"
echo

# Prompt for the Fivetran Account Name
read -p "Enter your Fivetran Account Name [MDS_SNOWFLAKE_HOL]: " ACCOUNT_NAME
ACCOUNT_NAME=${ACCOUNT_NAME:-"MDS_SNOWFLAKE_HOL"}

# Fetch the API key using the map structure
API_KEY=$(jq -r ".fivetran.api_keys[\"$ACCOUNT_NAME\"]" "$ROOT_CONFIG_PATH")
if [[ "$API_KEY" == "null" || -z "$API_KEY" ]]; then
    echo -e "${YELLOW}Error: API key not found for account '$ACCOUNT_NAME' in $ROOT_CONFIG_PATH!${NC}"
    exit 1
fi

# Prompt for the Destination Name with default
read -p "Enter your Fivetran Destination Name [NEW_SALES_ENG_HANDS_ON_LAB]: " DESTINATION_NAME
DESTINATION_NAME=${DESTINATION_NAME:-"NEW_SALES_ENG_HANDS_ON_LAB"}

# Prompt for the Connector Name
read -p "Enter a unique Fivetran Connection Name [my_new_fivetran_custom_connection]: " CONNECTION_NAME
CONNECTION_NAME=${CONNECTION_NAME:-"my_new_fivetran_custom_connection"}

echo
echo -e "${BOLD}Deployment Configuration:${NC}"
echo -e "  Account:     ${CYAN}$ACCOUNT_NAME${NC}"
echo -e "  Destination: ${CYAN}$DESTINATION_NAME${NC}"
echo -e "  Connection:  ${CYAN}$CONNECTION_NAME${NC}"
echo

# Deploy the connector using .venv2
echo -e "${BOLD}Deploying connection...${NC}"
echo -e "${CYAN}Running command:${NC} ~/.venv2/bin/fivetran deploy --api-key (hidden) --destination $DESTINATION_NAME --connection $CONNECTION_NAME --configuration configuration.json"

~/.venv2/bin/fivetran deploy \
  --api-key "$API_KEY" \
  --destination "$DESTINATION_NAME" \
  --connection "$CONNECTION_NAME" \
  --configuration configuration.json

# Final output
if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}${BOLD}✓ Deployment completed successfully!${NC}"
else
    echo -e "\n${YELLOW}⚠ Deployment encountered issues. Check the output above.${NC}"
    exit 1
fi
