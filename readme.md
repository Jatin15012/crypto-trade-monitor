REAL-TIME CRYPTO TRADE MONITOR

PROJECT OVERVIEW

A real-time cryptocurrency monitoring system built using Python, Apache Kafka, SQLite and Streamlit.

The application collects live Bitcoin (BTCUSDT) and Ethereum (ETHUSDT) trades from Binance, processes the data, detects unusual price movements and displays the results on an interactive dashboard.

Business Objective:
Help monitoring teams identify unusual cryptocurrency price movements while monitoring the health and performance of the data pipeline.

ARCHITECTURE

Binance WebSocket
|
v
producer.py
|
v
Kafka (trades topic)
|
v
consumer.py
|
v
SQLite (trades.db)
|
v
dashboard.py
|
v
Streamlit Dashboard

TECHNOLOGIES

* Python: Main programming language.
* Binance WebSocket: Provides real-time trade data.
* Apache Kafka: Transfers and buffers streaming messages.
* SQLite: Stores trades, alerts and counters.
* Streamlit: Displays the monitoring dashboard.
* Docker Compose: Runs Kafka locally.

MAIN COMPONENTS

1. producer.py
    Connects to Binance, receives live trades and sends them to Kafka. It also handles automatic reconnection.
2. Kafka
    Stores incoming trade messages in the “trades” topic until they are consumed.
3. consumer.py
    Reads messages from Kafka, handles duplicates, calculates rolling averages, detects unusual price movements and stores results in SQLite.
4. SQLite
    Stores trade records, alerts and monitoring information.
5. dashboard.py
    Displays live prices, charts, alerts, throughput, latency and system health.

HOW TO RUN

Requirements:

* Python 3
* Docker Desktop

1. Install dependencies:
    pip install -r requirements.txt
2. Start Kafka:
    docker compose up -d
3. Wait approximately 15 seconds, then create the topic:
    docker exec kafka /opt/kafka/bin/kafka-topics.sh –create –topic trades –bootstrap-server localhost:9092
4. Open three separate terminals and run:
    Terminal 1:
    python3 producer.py
    Terminal 2:
    python3 consumer.py
    Terminal 3:
    streamlit run dashboard.py
5. Open the dashboard:
    http://localhost:8501

To stop the application, press Ctrl+C in each terminal and run:

docker compose down

DATA PROCESSING AND CALCULATIONS

* Rolling Average: Calculates the average of the previous 50 trades separately for BTC and ETH. The current trade is excluded.
* Price Deviation: Measures the percentage difference between the current price and the rolling average.
    Formula:
    ((Current Price - Rolling Average) / Rolling Average) * 100
* Anomaly Alerts: An alert is generated when the absolute deviation reaches or exceeds 0.01%. Only one alert is generated per ongoing episode for each cryptocurrency.
* Trading Volume: Calculated as the sum of price multiplied by quantity, measured in USDT.
* Throughput: Number of trades processed in the last 60 seconds divided by 60.
* Latency: Average time difference between the Binance trade timestamp and consumer processing time, measured over the latest 200 trades.

FAULT TOLERANCE AND RECOVERY

* The producer automatically reconnects if the Binance WebSocket connection is lost.
* Exponential backoff is used: 1, 2, 4, 8 seconds and so on, up to 60 seconds.
* The WebSocket sends pings every 20 seconds with a 10-second timeout.
* The dashboard displays a NO LIVE DATA warning if no new trade arrives for more than 10 seconds.
* Kafka offsets are committed after database insertion, in batches of 100 trades or every 2 seconds.

Network Failure Test:
Wi-Fi was switched off and on to test reconnection. The interruption lasted approximately 87 seconds, during which around 9,831 BTC trades were missed. The producer reconnected automatically, but missed trades were not recovered because the public WebSocket feed does not replay them.

IDEMPOTENCY AND DUPLICATE HANDLING

Kafka may deliver the same message more than once.

To prevent duplicate database records, the project uses:

* A composite primary key: (symbol, trade_id).
* SQLite INSERT OR IGNORE.

This ensures that processing the same trade again does not create another database record.

Duplicate Replay Test:

* Duplicate messages skipped: 47,023
* New rows inserted: 0
* Database row count remained: 47,023
* No duplicate trade ID pairs were found.

A later database check showed 1,210,723 stored trades with no duplicate (symbol, trade_id) pairs.

The system uses at-least-once-style processing with idempotent database insertion. It does not provide complete end-to-end exactly-once processing.

PERFORMANCE RESULTS

* Throughput: Approximately 35–47 trades/second in calm periods and around 200 trades/second in busy periods.
* Average Latency: Approximately 130–210 milliseconds.
* Latest observed average latency: 149 milliseconds.
* Reliability: Automatic WebSocket reconnection tested successfully.
* Accuracy: Price deviation calculations manually verified.

Scalability has been considered but not demonstrated. The current implementation uses one Kafka broker, one partition and one consumer.

LIMITATIONS

* Only one Kafka broker and one partition are used.
* SQLite is suitable for this demonstration but not large-scale production traffic.
* Trades missed during network outages are not automatically recovered.
* Rolling-average data is stored in memory and needs 50 new trades per coin after a consumer restart.
* Individual Kafka message delivery is not confirmed by the producer.
* Kafka broker failure has not been tested.
* The 0.01% alert threshold is a demonstration setting and can generate frequent alerts.
* Dashboard counters reset when the consumer restarts.

PROJECT FILES

* producer.py
* consumer.py
* dashboard.py
* docker-compose.yml
* requirements.txt
* README.md
* prompts.txt

AI ASSISTANCE

This project was developed with assistance from Claude. I defined the project idea and requirements, worked through the implementation in smaller steps, and used AI assistance to understand concepts and troubleshoot problems. I ran and tested the implementation myself.

The prompts used during development are documented in prompts.txt.

CONCLUSION

This project demonstrates a real-time data engineering pipeline that collects, processes, stores and visualises live cryptocurrency trades.

It covers important concepts such as Kafka streaming, rolling averages, anomaly detection, fault tolerance, idempotency, offset management and performance monitoring.