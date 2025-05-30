#!/bin/bash

source ~/.venv2/bin/activate

# Path to your files
CONFIG_FILE="configuration.json"
CONNECTOR_FILE="connector.py"
DEBUG_OUTPUT_FILE="debug_output.tmp"
WAREHOUSE_DB="files/warehouse.db"

# Colors and formatting
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Check for required files
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${YELLOW}Error: Configuration file '$CONFIG_FILE' not found.${NC}"
    exit 1
fi

if [ ! -f "$CONNECTOR_FILE" ]; then
    echo -e "${YELLOW}Error: Connector file '$CONNECTOR_FILE' not found.${NC}"
    exit 1
fi

# Extract all table names
TABLE_NAMES=$(grep '"table"' "$CONNECTOR_FILE" | sed -E 's/.*"table": *"([^"]+)".*/\1/')

echo -e "${GREEN}✓ Detected table names:${NC}"
echo "$TABLE_NAMES" | sed 's/^/  - /'

# Display banner
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo -e "${BLUE}${BOLD}         Fivetran Connector Debug & Reset Script          ${NC}"
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo

# Confirm
echo -e "${BOLD}This will reset your connector, delete current state and warehouse.db files.${NC}"
read -p "Do you want to continue? (Y/N): " -n 1 -r USER_CONFIRM
echo
if [[ ! $USER_CONFIRM =~ ^[Yy]$ ]] && [[ ! -z $USER_CONFIRM ]]; then
    echo -e "${YELLOW}Operation cancelled by user.${NC}"
    exit 0
fi

# Step 1: Reset
echo -e "${BOLD}Step 1: ${NC}Resetting Fivetran connector..."
echo -e "${CYAN}Running command:${NC} ~/.venv2/bin/fivetran reset"
yes Y | ~/.venv2/bin/fivetran reset
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Reset failed. Exiting.${NC}"
    exit 1
fi

echo -e "\n${GREEN}✓ Reset successful${NC}"

# Step 2: Run debug
echo -e "${BOLD}Step 2: ${NC}Running debug with configuration..."
echo -e "${CYAN}Running command:${NC} ~/.venv2/bin/fivetran debug --configuration $CONFIG_FILE"
echo -e "${YELLOW}(Real-time output will be displayed below)${NC}\n"
export PYTHONUNBUFFERED=1
~/.venv2/bin/fivetran debug --configuration "$CONFIG_FILE" | tee "$DEBUG_OUTPUT_FILE"

echo -e "\n${GREEN}✓ Debug completed${NC}"

# Step 3: Query warehouse.db
echo -e "${BOLD}Step 3: ${NC}Querying sample data from DuckDB..."
echo

if [ -f "$WAREHOUSE_DB" ]; then
    if command -v duckdb >/dev/null 2>&1; then
        for TABLE in $TABLE_NAMES; do
            echo -e "${CYAN}Running query:${NC} SELECT * FROM tester.${TABLE} LIMIT 5;"
            duckdb "$WAREHOUSE_DB" "SELECT * FROM tester.${TABLE} LIMIT 5;" || echo -e "${YELLOW}⚠️  Table 'tester.${TABLE}' not found or error in query${NC}"
            echo
        done
    else
        echo -e "${YELLOW}DuckDB command-line tool not found. Install it or run manually:${NC}"
        for TABLE in $TABLE_NAMES; do
            echo "duckdb \"$WAREHOUSE_DB\" \"SELECT * FROM tester.${TABLE} ORDER BY record_id LIMIT 5;\""
        done
    fi
else
    echo -e "${YELLOW}warehouse.db not found. Looking for alternatives...${NC}"
    FOUND_DB=$(find . -name "warehouse.db" -type f | head -n 1)
    if [ -n "$FOUND_DB" ]; then
        echo "Found at: $FOUND_DB"
        for TABLE in $TABLE_NAMES; do
            echo "duckdb \"$FOUND_DB\" \"SELECT * FROM tester.${TABLE} ORDER BY record_id LIMIT 5;\""
        done
    else
        echo "No warehouse.db file found."
    fi
fi

# Final Summary
echo
echo -e "${BLUE}${BOLD}==================== OPERATION SUMMARY ====================${NC}"
SUMMARY=$(grep -A8 "SYNC PROGRESS:" "$DEBUG_OUTPUT_FILE")
[ -n "$SUMMARY" ] && echo "$SUMMARY" | sed 's/^/  /' || echo "  No summary found."

LAST_CHECKPOINT=$(grep -o "Checkpoint: .*" "$DEBUG_OUTPUT_FILE" | tail -n 1 | sed 's/Checkpoint: //')
echo -e "${BLUE}${BOLD}====================================================================${NC}"
echo -e "\n${GREEN}${BOLD}✓ Debug and reset operations completed.${NC}"
[ -n "$LAST_CHECKPOINT" ] && echo -e "${YELLOW}Next sync state:${NC} $LAST_CHECKPOINT" || echo -e "${YELLOW}Next sync state:${NC} No checkpoint found."

rm -f "$DEBUG_OUTPUT_FILE"
