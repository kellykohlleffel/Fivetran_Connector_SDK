#!/bin/bash

# Path to your files
CONFIG_FILE="configuration.json"
CONNECTOR_FILE="connector.py"
DEBUG_OUTPUT_FILE="debug_output.tmp"

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

# Extract table name from connector.py (POSIX-compatible)
TABLE_NAME=$(grep '"table"' "$CONNECTOR_FILE" | sed -E 's/.*"table": *"([^"]+)".*/\1/' | head -n 1)

echo -e "${GREEN}✓ Detected table name: $TABLE_NAME${NC}"

# Display banner
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo -e "${BLUE}${BOLD}         Fivetran Connector Debug & Reset Script          ${NC}"
echo -e "${BLUE}${BOLD}===========================================================${NC}"
echo

# Confirm
echo -e "${BOLD}This will reset your connector, delete current state and warehouse.db files.${NC}"
read -p "Do you want to continue? (Y/n): " -n 1 -r USER_CONFIRM
echo
if [[ ! $USER_CONFIRM =~ ^[Yy]$ ]] && [[ ! -z $USER_CONFIRM ]]; then
    echo -e "${YELLOW}Operation cancelled by user.${NC}"
    exit 0
fi

# Step 1: Reset
echo -e "${BOLD}Step 1: ${NC}Resetting Fivetran connector..."
yes Y | fivetran reset

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Reset failed. Exiting.${NC}"
    exit 1
fi

echo -e "\n${GREEN}✓ Reset successful${NC}"
echo -e "${BOLD}Step 2: ${NC}Running debug with configuration..."
echo -e "${YELLOW}(Real-time output will be displayed below)${NC}\n"
export PYTHONUNBUFFERED=1
fivetran debug --configuration "$CONFIG_FILE" | tee "$DEBUG_OUTPUT_FILE"

echo -e "\n${GREEN}✓ Debug completed${NC}"
echo -e "${BOLD}Step 3: ${NC}Querying sample data from DuckDB..."
echo

# Query warehouse.db
WAREHOUSE_DB="files/warehouse.db"
if [ -f "$WAREHOUSE_DB" ]; then
    if command -v duckdb >/dev/null 2>&1; then
        echo -e "${CYAN}Running query:${NC} SELECT * FROM tester.${TABLE_NAME} ORDER BY record_id LIMIT 5;"
        echo
        duckdb "$WAREHOUSE_DB" "SELECT * FROM tester.${TABLE_NAME} ORDER BY record_id LIMIT 5;"
    else
        echo -e "${YELLOW}DuckDB command-line tool not found. Install it or run this manually:${NC}"
        echo "duckdb \"$WAREHOUSE_DB\" \"SELECT * FROM tester.${TABLE_NAME} ORDER BY record_id LIMIT 5;\""
    fi
else
    echo -e "${YELLOW}warehouse.db not found. Looking for alternatives...${NC}"
    FOUND_DB=$(find . -name "warehouse.db" -type f | head -n 1)
    if [ -n "$FOUND_DB" ]; then
        echo "Found at: $FOUND_DB"
        echo "duckdb \"$FOUND_DB\" \"SELECT * FROM tester.${TABLE_NAME} ORDER BY record_id LIMIT 5;\""
    else
        echo "No warehouse.db file found."
    fi
fi

# Display sync summary
echo
echo -e "${BLUE}${BOLD}==================== OPERATION SUMMARY ====================${NC}"
SUMMARY=$(grep -A8 "SYNC PROGRESS:" "$DEBUG_OUTPUT_FILE")
[ -n "$SUMMARY" ] && echo "$SUMMARY" | sed 's/^/  /' || echo "  No summary found."

# Display checkpoint
LAST_CHECKPOINT=$(grep -o "Checkpoint: .*" "$DEBUG_OUTPUT_FILE" | tail -n 1 | sed 's/Checkpoint: //')
echo -e "${BLUE}${BOLD}====================================================================${NC}"
echo -e "\n${GREEN}${BOLD}✓ Debug and reset operations completed.${NC}"
[ -n "$LAST_CHECKPOINT" ] && echo -e "${YELLOW}Next sync state:${NC} $LAST_CHECKPOINT" || echo -e "${YELLOW}Next sync state:${NC} No checkpoint found."

# Cleanup
rm -f "$DEBUG_OUTPUT_FILE"