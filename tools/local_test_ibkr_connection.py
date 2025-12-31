from ib_insync import IB
import os

HOST = os.environ.get("IBKR_HOST", "127.0.0.1")
PORT = int(os.environ.get("IBKR_PORT", "4001"))     # LIVE gateway is often 4001
CLIENT_ID = int(os.environ.get("IBKR_CLIENT_ID", "7"))

ib = IB()
ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
print("connected:", ib.isConnected())
ib.disconnect()
