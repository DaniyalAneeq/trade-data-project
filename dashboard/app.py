import json
import time

import pandas as pd
import plotly.graph_objects as go
import redis
import streamlit as st

st.set_page_config(page_title="Live Crypto Trades", layout="wide")


@st.cache_resource
def get_redis():
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


r = get_redis()

st.title("Live Crypto Trades")
st.caption("Binance → Kafka → dashboard consumer → Redis → this page (refreshes every 2 s)")

symbols = sorted(r.smembers("symbols"))
if not symbols:
    st.warning("No data yet. Is dashboard_consumer.py running?")
    st.stop()

symbol = st.selectbox("Coin", symbols)


@st.fragment(run_every=2)
def live_view(symbol):
    # ---- Health: is the consumer alive? ----
    hb = r.get("dashboard:heartbeat")
    age = (time.time() * 1000 - int(hb)) / 1000 if hb else None
    if age is None or age > 10:
        st.error(f"Dashboard consumer is not updating (last heartbeat: "
                 f"{'never' if age is None else f'{age:.0f}s ago'}). Data below is stale.")
    else:
        st.success(f"Live: last update {age:.1f}s ago")

    # ---- Load data from Redis ----
    latest = r.hgetall(f"latest:{symbol}")
    live = r.get(f"candle_live:{symbol}")
    history = [json.loads(x) for x in r.lrange(f"candles:{symbol}", 0, 59)]  # last 60 min
    rows = list(reversed(history)) + ([json.loads(live)] if live else [])
    df = pd.DataFrame(rows)

    # ---- Headline numbers ----
    c1, c2, c3, c4 = st.columns(4)
    if latest:
        price = float(latest["price"])
        delta = None
        if not df.empty:
            first_open = df["open"].iloc[0]
            delta = f"{(price - first_open) / first_open * 100:+.2f}% (window)"
        c1.metric("Last price", f"{price:,.2f}", delta)
        c4.metric("Latency (trade → Redis)", f"{int(latest['latency_ms'])} ms")
    if live:
        lc = json.loads(live)
        vwap = lc["quote_volume"] / lc["volume"] if lc["volume"] else 0
        c2.metric("Trades this minute", f"{lc['trades']:,}")
        c3.metric("VWAP this minute", f"{vwap:,.2f}")

    # ---- Candlestick chart ----
    if not df.empty:
        df["time"] = pd.to_datetime(df["minute"], unit="ms", utc=True).dt.tz_convert("Asia/Karachi")
        fig = go.Figure(go.Candlestick(x=df["time"], open=df["open"], high=df["high"],
                                       low=df["low"], close=df["close"], name=symbol))
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10),
                          xaxis_rangeslider_visible=False, title=f"{symbol}: 1-minute candles")
        st.plotly_chart(fig)
        st.bar_chart(df.set_index("time")["volume"], height=150)

    # ---- Recent trades ----
    trades = [json.loads(x) for x in r.lrange(f"trades:{symbol}", 0, 19)]
    if trades:
        tdf = pd.DataFrame(trades)
        tdf["time"] = pd.to_datetime(tdf["time"], unit="ms", utc=True) \
                        .dt.tz_convert("Asia/Karachi").dt.strftime("%H:%M:%S.%f").str[:-3]
        st.subheader("Recent trades")
        st.dataframe(tdf[["time", "side", "price", "quantity"]], hide_index=True)


live_view(symbol)
