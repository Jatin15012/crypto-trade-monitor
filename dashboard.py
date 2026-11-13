import sqlite3
import time
import pandas as pd
import plotly.express as px
import streamlit as st

DB_FILE = "trades.db"
STALE_AFTER_SECONDS = 10
REFRESH_SECONDS = 2
WINDOW_SIZE = 50
CHART_POINTS = 300
THRESHOLD_PERCENT = 0.01
SYMBOLS = ["BTCUSDT", "ETHUSDT"]

st.set_page_config(page_title="Crypto Monitor", layout="wide")
st.title("Real-Time Crypto Trade Monitor")

db = sqlite3.connect(DB_FILE, timeout=5)


def one_value(sql, params=()):
    row = db.execute(sql, params).fetchone()
    if row is None or row[0] is None:
        return 0
    return row[0]


# Section 1: live market overview
st.subheader("Live market overview")
columns = st.columns(4)
for position, symbol in enumerate(SYMBOLS):
    latest_price = one_value(
        "SELECT price FROM trades WHERE symbol = ? ORDER BY trade_id DESC LIMIT 1",
        (symbol,),
    )
    columns[position].metric(symbol + " latest price", f"{latest_price:,.2f}")

total_trades = one_value("SELECT COUNT(*) FROM trades")
total_volume = one_value("SELECT SUM(price * quantity) FROM trades")
columns[2].metric("Total trades stored", f"{total_trades:,}")
columns[3].metric("Total volume (USDT)", f"{total_volume:,.0f}")

# Section 2: price analytics
st.subheader("Price analytics")
chart_columns = st.columns(2)
for position, symbol in enumerate(SYMBOLS):
    prices = pd.read_sql_query(
        "SELECT price FROM trades WHERE symbol = ? ORDER BY trade_id DESC LIMIT ?",
        db,
        params=(symbol, CHART_POINTS),
    )
    prices = prices.iloc[::-1].reset_index(drop=True)
    prices["rolling_avg"] = prices["price"].shift(1).rolling(WINDOW_SIZE).mean()

    with chart_columns[position]:
        st.markdown(f"**{symbol}: price and {WINDOW_SIZE}-trade rolling average**")
        fig = px.line(prices, y=["price", "rolling_avg"])
        fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
        fig.update_xaxes(title="last 300 trades")
        fig.update_yaxes(title="price (USDT)")
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{symbol}")

        average_price = one_value(
            "SELECT AVG(price) FROM trades WHERE symbol = ?", (symbol,)
        )
        numbers = st.columns(3)
        numbers[0].metric("Average price (all stored trades)", f"{average_price:,.2f}")
        if len(prices) > WINDOW_SIZE:
            latest = prices["price"].iloc[-1]
            rolling = prices["rolling_avg"].iloc[-1]
            deviation = (latest - rolling) / rolling * 100
            numbers[1].metric("Rolling average", f"{rolling:,.2f}")
            numbers[2].metric("Deviation", f"{deviation:+.4f}%")

# Section 3: anomaly detection
st.subheader("Anomaly alerts")
st.caption(
    f"Simple monitoring rule: alert when a price is at least {THRESHOLD_PERCENT}% "
    f"away from its {WINDOW_SIZE}-trade rolling average. This is not a prediction."
)
alerts = pd.read_sql_query(
    "SELECT symbol, price, rolling_avg, deviation_percent, detected_at "
    "FROM alerts ORDER BY rowid DESC LIMIT 10",
    db,
)
if alerts.empty:
    st.info("No anomalies detected yet.")
else:
    alerts["detected_at"] = alerts["detected_at"].apply(
        lambda ms: time.strftime("%H:%M:%S", time.localtime(ms / 1000))
    )
    st.dataframe(alerts)

# Section 4: system health
st.subheader("System health")
now_ms = int(time.time() * 1000)
last_received = one_value("SELECT MAX(received_at) FROM trades")
seconds_since_last = (now_ms - last_received) / 1000

if last_received > 0 and seconds_since_last < STALE_AFTER_SECONDS:
    st.success("LIVE: trades are arriving")
else:
    st.error("NO LIVE DATA: no new trade for more than 10 seconds")

trades_last_minute = one_value(
    "SELECT COUNT(*) FROM trades WHERE received_at >= ?", (now_ms - 60000,)
)
average_latency_ms = one_value(
    "SELECT AVG(received_at - trade_time) FROM "
    "(SELECT received_at, trade_time FROM trades ORDER BY rowid DESC LIMIT 200)"
)
last_time_text = time.strftime("%H:%M:%S", time.localtime(last_received / 1000))

health = st.columns(3)
health[0].metric("Last trade received", last_time_text)
health[1].metric("Throughput (trades/sec, last 60s)", f"{trades_last_minute / 60:.1f}")
health[2].metric("Average latency (ms, last 200)", f"{average_latency_ms:,.0f}")

try:
    stats_row = db.execute(
        "SELECT messages_processed, duplicates_skipped, alerts_raised FROM stats WHERE id = 1"
    ).fetchone()
except sqlite3.OperationalError:
    stats_row = None
processed, duplicates, alerts_raised = stats_row if stats_row else (0, 0, 0)

counters = st.columns(3)
counters[0].metric("Messages processed (since consumer start)", f"{processed:,}")
counters[1].metric("Duplicates detected (since consumer start)", f"{duplicates:,}")
counters[2].metric("Alerts raised (since consumer start)", f"{alerts_raised:,}")

db.close()
time.sleep(REFRESH_SECONDS)
st.rerun()