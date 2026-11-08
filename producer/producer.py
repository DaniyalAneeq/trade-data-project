import json
import logging
import os
import signal
import time
from pathlib import Path

import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------- Settings ----------
# Reads EC2_PUBLIC_DNS from infra/.env (created in Step 2)
load_dotenv(Path(__file__).resolve().parent.parent / "infra" / ".env")

SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
WS_HOST = os.getenv("BINANCE_WS_HOST", "wss://data-stream.binance.vision")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades")
BOOTSTRAP = f"{os.environ['EC2_PUBLIC_DNS']}:29092"

# One connection, many symbols ("combined stream")
WS_URL = f"{WS_HOST}/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# ---------- Kafka producer ----------
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "binance-trade-producer",
    "acks": "all",                # wait until Kafka has stored the message
    "enable.idempotence": True,   # retries never create duplicates
    "compression.type": "lz4",    # smaller messages, cheap on CPU
    "linger.ms": 50,              # group messages for up to 50 ms -> fewer requests
})

stats = {"sent": 0, "failed": 0, "gaps": 0}
last_trade_id = {}
running = True


def on_delivery(err, msg):
    """Called by Kafka for every message: success or failure."""
    if err:
        stats["failed"] += 1
        log.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        stats["sent"] += 1


def to_record(t):
    """Turn Binance's short field names into clear ones."""
    return {
        "symbol": t["s"],
        "trade_id": t["t"],
        "price": t["p"],              # kept as text: no float rounding
        "quantity": t["q"],
        "trade_time": t["T"],         # when the trade happened (ms)
        "event_time": t["E"],         # when Binance sent it (ms)
        "is_buyer_maker": t["m"],
        "ingested_at": int(time.time() * 1000),  # when we received it (ms)
    }


def check_gap(rec):
    """Trade IDs go up by 1 per symbol. A jump means we missed trades."""
    prev = last_trade_id.get(rec["symbol"])
    if prev is not None and rec["trade_id"] > prev + 1:
        stats["gaps"] += 1
        log.warning("GAP %s: missed %d trades (%d -> %d)",
                    rec["symbol"], rec["trade_id"] - prev - 1, prev, rec["trade_id"])
    last_trade_id[rec["symbol"]] = rec["trade_id"]


def send(rec):
    while True:
        try:
            producer.produce(TOPIC, key=rec["symbol"], value=json.dumps(rec),
                             on_delivery=on_delivery)
            break
        except BufferError:
            # Local buffer is full (e.g. Kafka is down) -> wait and retry
            log.warning("Local buffer full, waiting for Kafka...")
            producer.poll(1)
    producer.poll(0)  # triggers on_delivery callbacks


def stop(*_):
    global running
    running = False
    log.info("Stop requested...")


signal.signal(signal.SIGINT, stop)   # Ctrl+C
signal.signal(signal.SIGTERM, stop)  # docker/systemd stop


def run():
    backoff = 1
    last_log = time.time()
    while running:
        ws = None
        try:
            log.info("Connecting to %s", WS_URL)
            ws = websocket.create_connection(WS_URL, timeout=30)
            log.info("Connected. Kafka: %s  Topic: %s", BOOTSTRAP, TOPIC)
            backoff = 1

            while running:
                raw = ws.recv()      # pings from Binance are answered automatically
                if not raw:
                    continue
                data = json.loads(raw).get("data", {})

                if data.get("e") == "serverShutdown":
                    log.warning("Binance is restarting this server, reconnecting")
                    break
                if data.get("e") != "trade":
                    continue

                rec = to_record(data)
                check_gap(rec)
                send(rec)

                if time.time() - last_log >= 10:
                    log.info("sent=%d failed=%d gaps=%d waiting=%d",
                             stats["sent"], stats["failed"], stats["gaps"], len(producer))
                    last_log = time.time()

        except Exception as e:
            log.error("Connection problem: %s", e)
        finally:
            if ws:
                ws.close()

        if running:
            log.info("Reconnecting in %ds", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)   # 1, 2, 4, 8 ... up to 60s

    log.info("Sending remaining messages to Kafka...")
    left = producer.flush(10)
    log.info("Stopped. sent=%d failed=%d unsent=%d", stats["sent"], stats["failed"], left)


if __name__ == "__main__":
    run()
