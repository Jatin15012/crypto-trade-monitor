import json
import time
import websocket
from confluent_kafka import Producer

URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
MAX_WAIT_SECONDS = 60

producer = Producer({"bootstrap.servers": "localhost:9092"})
wait_seconds = 1
keep_running = True

def on_open(ws):
    global wait_seconds
    print("Connected to Binance")
    wait_seconds = 1

def on_message(ws, message):
    wrapper = json.loads(message)
    trade = wrapper["data"]
    simple_trade = {
        "trade_id": trade["t"],
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "trade_time": trade["T"],
    }
    producer.produce("trades", key=simple_trade["symbol"], value=json.dumps(simple_trade))
    producer.poll(0)
    print(simple_trade)

def on_error(ws, error):
    global keep_running
    if isinstance(error, KeyboardInterrupt):
        keep_running = False
    print("Error:", error)

def on_close(ws, close_status_code, close_message):
    print("Connection closed")

try:
    while keep_running:
        ws = websocket.WebSocketApp(
            URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        if not keep_running:
            break
        print(f"Disconnected. Reconnecting in {wait_seconds} seconds...")
        time.sleep(wait_seconds)
        wait_seconds = min(wait_seconds * 2, MAX_WAIT_SECONDS)
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    producer.flush()