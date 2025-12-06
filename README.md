# Zerodha to AmiBroker Connector

This project provides a robust, real-time data bridge between **Zerodha Kite** and **AmiBroker**. It fetches live market data (ticks) from Zerodha, processes them into OHLCV candles, and relays them to AmiBroker via a local WebSocket server.

## 🚀 Features

-   **Real-time Data**: Streams live quotes from Zerodha Kite Ticker.
-   **Mock Mode**: Automatically falls back to generated mock data if Zerodha credentials are missing or connection fails.
-   **Dynamic Symbols**: Configure symbols via `symbols.yaml` without changing code.
-   **Relay Server**: Decouples data fetching from data consumption, allowing AmiBroker to reconnect without data loss.
-   **Backfill Support**: (Experimental) Can generate historical data for backfilling.
-   **Resilience**: Built-in auto-reconnection, circuit breakers, and error handling.

## 🛠️ Prerequisites

-   **Python 3.8+**
-   **Zerodha Kite Connect API Account** (API Key & Access Token)
-   **AmiBroker** (with a WebSocket-compatible plugin or client)

## 📦 Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd AmibrokerConnector
    ```

2.  **Install Dependencies**:
    It is recommended to use a virtual environment.
    ```bash
    pip install -r requirements.txt
    ```

## ⚙️ Configuration

### 1. API Credentials (`run.sh`)
The system uses environment variables for security. The `run.sh` script handles this for you.

Open `run.sh` and update the following lines with your actual Zerodha credentials:

```bash
API_KEY="your_zerodha_api_key"
ACCESS_TOKEN="your_zerodha_access_token"
```

> **Note**: If you leave these blank or invalid, the system will start in **Mock Mode** and generate simulated data.

### 2. Symbol Configuration (`symbols.yaml`)
Create or edit `symbols.yaml` in the project root to define which symbols to track.

Example `symbols.yaml`:
```yaml
symbols:
  - NIFTY 50
  - BANKNIFTY
  - RELIANCE
  - TCS
  - INFY
  - SBIN
```
*Ensure these symbols match exactly with Zerodha's trading symbols.*

## ▶️ How to Run

The easiest way to start the system is using the provided shell script:

```bash
./run.sh
```

This script will:
1.  Check for Python 3.
2.  Export your API credentials.
3.  Launch `amibrokerConnector.py` in `both` mode (running both the Relay Server and Zerodha Connector).

### Manual Execution
If you prefer running manually or want to run components separately:

**Run everything (Relay + Connector):**
```bash
python3 amibrokerConnector.py --component both --api-key "your_key" --access-token "your_token"
```

**Run only Relay Server:**
```bash
python3 amibrokerConnector.py --component relay
```

**Run only Connector (requires running Relay first):**
```bash
python3 amibrokerConnector.py --component zerodha --api-key "your_key" --access-token "your_token"
```

## ✅ Verification

To verify that data is flowing correctly, you can use the included verification script.

1.  Start the system (`./run.sh`).
2.  Open a **new terminal window**.
3.  Run:
    ```bash
    python3 verify_relay.py
    ```

You should see output indicating connection success and a stream of data packets:
```text
Connecting to ws://localhost:10101...
Sent 'rolerecv'
Received: {"status": "connected", ...}
Received Batch of 10 records
Sample: {'n': 'RELIANCE', 'o': 2450.0, ...}
SUCCESS: Data is flowing correctly from Relay!
```

## 📂 Project Structure

-   `amibrokerConnector.py`: The core application. Contains logic for the Relay Server, Zerodha Client, and Candle Generation.
-   `run.sh`: Startup script to set env vars and launch the app.
-   `symbols.yaml`: Configuration file for trading symbols.
-   `verify_relay.py`: Utility script to test the WebSocket data stream.
-   `requirements.txt`: Python dependencies.
-   `relay.py`: (Legacy) Standalone relay server implementation.

## 🐛 Troubleshooting

-   **"python3 could not be found"**: Ensure Python 3 is installed and added to your system PATH.
-   **Connection Refused**: Make sure the Relay Server is running (port 10101 by default).
-   **403 Forbidden (Zerodha)**: Your API Key or Access Token is invalid or expired. The system will fall back to Mock Mode. Update `run.sh` with fresh credentials.
-   **No Data in AmiBroker**: Verify that the symbol names in `symbols.yaml` match exactly what AmiBroker expects or what Zerodha provides.

## 🙏 Credits / Acknowledgments

WS-RTD Plugin by NSM51 — for providing the WebSocket-to-AmiBroker interface and ongoing beta support.

Tai Pan Realtime (TPRAccess) — for access to Euronext and European data used in development and testing.

AmiBroker Community — for collaboration, testing feedback, and inspiration for extending real-time workflows.

Special thanks to contributors and testers who helped refine backfill, INFO, and bridge synchronization logic.

© 2025 — TP-WSRTD-Client (Open Integration Project)

## 📝 License
[Include License Information Here]
