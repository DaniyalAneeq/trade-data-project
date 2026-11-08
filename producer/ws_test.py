import json, websocket

for url in ["wss://data-stream.binance.vision/ws/btcusdt@trade",
            "wss://stream.binance.com:9443/ws/btcusdt@trade"]:
    try:
        ws = websocket.create_connection(url, timeout=10)
        m = json.loads(ws.recv())
        print(f"OK   {url}\n     trade_id={m['t']} price={m['p']} qty={m['q']}")
        ws.close()
    except Exception as e:
        print(f"FAIL {url}\n     {type(e).__name__}: {e}")
