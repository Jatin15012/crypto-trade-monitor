import json
import sqlite3
import time
from collections import deque
from confluent_kafka import Consumer

GROUP_ID = "trade-processor-v2"
BATCH_SIZE = 100
MAX_SECONDS_BETWEEN_SAVES = 2
WINDOW_SIZE = 50
THRESHOLD_PERCENT = 0.01

db = sqlite3.connect("trades.db")
db.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        symbol TEXT,
        trade_id INTEGER,
        price REAL,
        quantity REAL,
        trade_time INTEGER,
        received_at INTEGER,
        PRIMARY KEY (symbol, trade_id)
    )
""")
db.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        symbol TEXT,
        trade_id INTEGER,
        price REAL,
        rolling_avg REAL,
        deviation_percent REAL,
        detected_at INTEGER,
        PRIMARY KEY (symbol, trade_id)
    )
""")
db.execute("""
    CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY,
        messages_processed INTEGER,
        duplicates_skipped INTEGER,
        alerts_raised INTEGER
    )
""")
db.commit()

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["trades"])

saved_count = 0
duplicate_count = 0
alert_count = 0
recent_prices = {}
in_alert = {}
pending = 0
last_msg = None
last_save_time = time.time()


def save_stats():
    db.execute(
        "INSERT OR REPLACE INTO stats VALUES (1, ?, ?, ?)",
        (saved_count + duplicate_count, duplicate_count, alert_count),
    )


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is not None and msg.error():
            print("Kafka error:", msg.error())
        elif msg is not None:
            trade = json.loads(msg.value().decode("utf-8"))
            received_at = int(time.time() * 1000)

            cursor = db.execute(
                "INSERT OR IGNORE INTO trades VALUES (?, ?, ?, ?, ?, ?)",
                (
                    trade["symbol"],
                    trade["trade_id"],
                    trade["price"],
                    trade["quantity"],
                    trade["trade_time"],
                    received_at,
                ),
            )
            if cursor.rowcount == 1:
                saved_count += 1

                symbol = trade["symbol"]
                price = trade["price"]
                if symbol not in recent_prices:
                    recent_prices[symbol] = deque(maxlen=WINDOW_SIZE)
                    in_alert[symbol] = False
                window = recent_prices[symbol]

                if len(window) == WINDOW_SIZE:
                    rolling_avg = sum(window) / WINDOW_SIZE
                    deviation_percent = (price - rolling_avg) / rolling_avg * 100
                    if abs(deviation_percent) >= THRESHOLD_PERCENT:
                        if not in_alert[symbol]:
                            db.execute(
                                "INSERT OR IGNORE INTO alerts VALUES (?, ?, ?, ?, ?, ?)",
                                (symbol, trade["trade_id"], price, rolling_avg,
                                 deviation_percent, received_at),
                            )
                            alert_count += 1
                            in_alert[symbol] = True
                    else:
                        in_alert[symbol] = False

                window.append(price)
            else:
                duplicate_count += 1

            pending += 1
            last_msg = msg

        time_is_up = time.time() - last_save_time >= MAX_SECONDS_BETWEEN_SAVES
        if pending > 0 and (pending >= BATCH_SIZE or time_is_up):
            save_stats()
            db.commit()
            consumer.commit(message=last_msg, asynchronous=False)
            pending = 0
            last_save_time = time.time()
            print(f"saved={saved_count} duplicates_skipped={duplicate_count} alerts={alert_count}")
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    if pending > 0:
        save_stats()
        db.commit()
        consumer.commit(message=last_msg, asynchronous=False)
    print(f"Final: saved={saved_count} duplicates_skipped={duplicate_count} alerts={alert_count}")
    consumer.close()
    db.close()