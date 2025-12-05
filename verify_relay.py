import asyncio
import websockets
import json
import sys

async def verify_relay():
    uri = "ws://localhost:10101"
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            # 1. Send Role
            await websocket.send("rolerecv")
            print("Sent 'rolerecv'")
            
            # 2. Receive Welcome
            welcome = await websocket.recv()
            print(f"Received: {welcome}")
            
            # 3. Listen for Data
            print("Listening for data (Ctrl+C to stop)...")
            count = 0
            while count < 5:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"Received Batch of {len(data)} records")
                if len(data) > 0:
                    print(f"Sample: {data[0]}")
                count += 1
            
            print("\nSUCCESS: Data is flowing correctly from Relay!")
            
    except Exception as e:
        print(f"\nFAILURE: Could not connect or receive data: {e}")
        print("Make sure amibrokerConnector.py is running.")

if __name__ == "__main__":
    try:
        asyncio.run(verify_relay())
    except KeyboardInterrupt:
        pass
