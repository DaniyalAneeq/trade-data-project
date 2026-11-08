import io
import json
import logging
import os
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# ---------- Settings ----------
load_dotenv(Path(__file__).resolve().parent.parent / "infra" / ".env")

BOOTSTRAP = f"{os.environ['EC2_PUBLIC_DNS']}:29092"
TOPIC = "crypto.trades"
DLQ_TOPIC = "crypto.trades.dlq"
BUCKET = os.environ["S3_BUCKET"]
REGION = os.getenv("AWS_REGION", "ap-south-1")
PREFIX = "raw/trades"
MAX_RECORDS = int(os.getenv("FLUSH_MAX_RECORDS", "50000"))
MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("storage-consumer")

# Fixed column types for the Parquet files
SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("trade_id", pa.int64()),
    ("price", pa.string()),          # text on purpose: exact value
    ("quantity", pa.string()),
    ("trade_time", pa.int64()),
    ("event_time", pa.int64()),
    ("is_buyer_maker", pa.bool_()),
    ("ingested_at", pa.int64()),
    ("kafka_partition", pa.int32()),  # where this row came from in Kafka
    ("kafka_offset", pa.int64()),
])

# ---------- Clients ----------
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 10, "mode": "standard"}))

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "s3-sink",
    "enable.auto.commit": False,      # WE decide when to commit (after S3 write)
    "auto.offset.reset": "earliest",  # first run: start from the oldest message
})

dlq = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})

buffer = []
batch_started = None
running = True
stats = {"files": 0, "records": 0, "dlq": 0}


def parse(msg):
    """Check the message and give each field its exact type. Raises if broken."""
    d = json.loads(msg.value())
    return {
        "symbol": str(d["symbol"]),
        "trade_id": int(d["trade_id"]),
        "price": str(d["price"]),
        "quantity": str(d["quantity"]),
        "trade_time": int(d["trade_time"]),
        "event_time": int(d["event_time"]),
        "is_buyer_maker": bool(d["is_buyer_maker"]),
        "ingested_at": int(d["ingested_at"]),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
    }


def send_to_dlq(msg, error):
    dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(),
                headers={"error": str(error)[:200],
                         "source": f"{msg.partition()}:{msg.offset()}"})
    dlq.poll(0)
    stats["dlq"] += 1
    log.warning("Bad message %d:%d sent to DLQ: %s", msg.partition(), msg.offset(), error)


def s3_key(symbol, hour):
    return (f"{PREFIX}/symbol={symbol}/dt={hour:%Y-%m-%d}/hour={hour:%H}/"
            f"part-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.parquet")


def flush():
    """Write the buffer to S3 (one file per symbol+hour), THEN commit."""
    global buffer, batch_started
    if not buffer:
        return

    groups = defaultdict(list)
    for r in buffer:
        hour = datetime.fromtimestamp(r["trade_time"] / 1000, tz=timezone.utc) \
                       .replace(minute=0, second=0, microsecond=0)
        groups[(r["symbol"], hour)].append(r)

    for (symbol, hour), rows in groups.items():
        out = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), out, compression="snappy")
        s3.put_object(Bucket=BUCKET, Key=s3_key(symbol, hour), Body=out.getvalue())
        stats["files"] += 1

    dlq.flush(5)                        # make sure DLQ messages are stored too
    consumer.commit(asynchronous=False)  # only now: "done up to here"

    stats["records"] += len(buffer)
    log.info("Flushed %d records into %d files, committed. Totals: files=%d records=%d dlq=%d",
             len(buffer), len(groups), stats["files"], stats["records"], stats["dlq"])
    buffer = []
    batch_started = None


def flush_with_retry():
    for attempt in range(1, 6):
        try:
            flush()
            return
        except Exception as e:
            log.error("Flush failed (attempt %d/5): %s", attempt, e)
            time.sleep(2 ** attempt)   # 2, 4, 8, 16, 32 s
    # Exit WITHOUT committing -> on restart we re-read these messages. Nothing lost.
    raise SystemExit("S3 keeps failing. Exiting without commit.")


def on_revoke(c, partitions):
    """If Kafka takes partitions away (e.g. a second consumer joins), save our work first."""
    log.info("Partitions revoked, flushing first")
    flush_with_retry()


def stop(*_):
    global running
    running = False
    log.info("Stop requested...")


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def run():
    global batch_started
    consumer.subscribe([TOPIC], on_revoke=on_revoke)
    log.info("Started. Kafka: %s -> s3://%s/%s (every %ds or %d records)",
             BOOTSTRAP, BUCKET, PREFIX, MAX_SECONDS, MAX_RECORDS)

    while running:
        msg = consumer.poll(1.0)

        if msg is not None:
            if msg.error():
                log.error("Kafka error: %s", msg.error())
            else:
                try:
                    buffer.append(parse(msg))
                    if batch_started is None:
                        batch_started = time.time()
                except Exception as e:
                    send_to_dlq(msg, e)

        if buffer and (len(buffer) >= MAX_RECORDS or time.time() - batch_started >= MAX_SECONDS):
            flush_with_retry()

    log.info("Shutting down: writing last batch...")
    flush_with_retry()
    dlq.flush(10)
    consumer.close()
    log.info("Stopped cleanly.")


if __name__ == "__main__":
    run()
