import json
import logging
import os
import signal
import time
from pathlib import Path

import redis
from confluent_kafka import Consumer
from dotenv import load_dotenv

# ---------- Settings ----------
load_dotenv(Path(__file__).resolve().parent.parent / "infra" / ".env")

BOOTSTRAP = f"{os.environ['EC2_PUBLIC_DNS']}:29092"
TOPIC = "crypto.trades"
CANDLES_KEEP = 180   # 3 hours of 1-minute candles per coin
TRADES_KEEP = 50     # last 50 trades per coin

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dashboard-consumer")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "dashboard",
    "auto.offset.reset": "latest",   # live view: start from NOW
    "enable.auto.commit": True,      # skipping a few trades on restart is OK here
})

candles = {}   # symbol -> the candle for the current minute
stats = {"processed": 0, "skipped": 0}
running = True


def new_candle(minute, price):
    return {"minute": minute, "open": price, "high": price, "low": price, "close": price,
            "volume": 0.0, "quote_volume": 0.0, "trades": 0}


def update(pipe, t, now_ms):
    sym = t["symbol"]
    price = float(t["price"])        # float is fine for display; exact values live in Snowflake
    qty = float(t["quantity"])
    minute = t["trade_time"] // 60000 * 60000

    c = candles.get(sym)
    if c is None or minute > c["minute"]:
        # New minute: save the finished candle, start a new one
        if c is not None:
            pipe.lpush(f"candles:{sym}", json.dumps(c))
            pipe.ltrim(f"candles:{sym}", 0, CANDLES_KEEP - 1)
        c = candles[sym] = new_candle(minute, price)
    elif minute < c["minute"]:
        return   # late trade from an already-closed minute: ignore for the live view

    c["high"] = max(c["high"], price)
    c["low"] = min(c["low"], price)
    c["close"] = price
    c["volume"] += qty
    c["quote_volume"] += price * qty   # used for VWAP
    c["trades"] += 1

    side = "SELL" if t["is_buyer_maker"] else "BUY"   # who took the price
    pipe.set(f"candle_live:{sym}", json.dumps(c))
    pipe.hset(f"latest:{sym}", mapping={
        "price": t["price"], "quantity": t["quantity"], "side": side,
        "trade_time": t["trade_time"], "latency_ms": now_ms - t["trade_time"],
    })
    pipe.lpush(f"trades:{sym}", json.dumps({
        "time": t["trade_time"], "price": t["price"], "quantity": t["quantity"], "side": side,
    }))
    pipe.ltrim(f"trades:{sym}", 0, TRADES_KEEP - 1)
    pipe.sadd("symbols", sym)


def stop(*_):
    global running
    running = False
    log.info("Stop requested...")


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def run():
    consumer.subscribe([TOPIC])
    log.info("Started. Kafka: %s -> Redis", BOOTSTRAP)
    last_log = time.time()

    while running:
        msgs = consumer.consume(num_messages=500, timeout=0.5)   # read in small batches
        now_ms = int(time.time() * 1000)
        pipe = r.pipeline(transaction=False)                      # one round-trip to Redis

        for m in msgs:
            if m.error():
                log.error("Kafka error: %s", m.error())
                continue
            try:
                update(pipe, json.loads(m.value()), now_ms)
                stats["processed"] += 1
            except Exception as e:
                stats["skipped"] += 1
                log.warning("Skipped bad message %d:%d: %s", m.partition(), m.offset(), e)

        pipe.set("dashboard:heartbeat", now_ms)   # "I'm alive" signal for the dashboard
        try:
            pipe.execute()
        except redis.ConnectionError as e:
            log.error("Redis unavailable, dropping this batch: %s", e)
            time.sleep(2)

        if time.time() - last_log >= 10:
            log.info("processed=%d skipped=%d", stats["processed"], stats["skipped"])
            last_log = time.time()

    consumer.close()
    log.info("Stopped cleanly.")


if __name__ == "__main__":
    run()
