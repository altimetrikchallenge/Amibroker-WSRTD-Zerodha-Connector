#!/bin/bash

# Replace these with your actual Zerodha credentials
# You can also set them as environment variables
API_KEY=""
ACCESS_TOKEN=""

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "python3 could not be found"
    exit 1
fi

echo "Starting Amibroker Connector..."
echo "Using API Key: $API_KEY"
# Don't print access token for security

# Run the connector
# We use --component both to run both Relay and Connector
python3 amibrokerConnector.py \
    --component both \
    --port 10101 \
    --api-key "$API_KEY" \
    --access-token "$ACCESS_TOKEN"
