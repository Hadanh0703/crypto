import clickhouse_connect
import random

try:
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

    ticker = 'BTCUSDT'
    start_time = 1710000000000
    interval = 300000

    print("Generating mock data...")
    fact_records = []
    ind_records = []

    base_price = 65000.0

    for i in range(100):
        open_time = start_time + (i * interval)
        base_price += (random.random() - 0.48) * 100
        
        op = base_price
        hi = base_price + 50
        lo = base_price - 50
        cl = base_price + (random.random() - 0.5) * 20
        vol = 1000.0 + random.random() * 500
        
        ma20 = cl * 1.001
        ma50 = cl * 0.99
        rsi = 40.0 + random.random() * 20
        bb_upper = cl * 1.02
        bb_lower = cl * 0.98
        
        fact_records.append([ticker, open_time, op, hi, lo, cl, vol, open_time + interval - 1, 0.0, 0, 0.0, 0.0, 0.0])
        ind_records.append([ticker, open_time, ma20, ma50, rsi, bb_upper, bb_lower, 0])

    print("Inserting facts...")
    client.insert('future_kline_5m', fact_records, column_names=['ticker', 'openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'])

    print("Inserting indicators...")
    client.insert('kline_indicators', ind_records, column_names=['ticker', 'openTime', 'ma20', 'ma50', 'rsi', 'bb_upper', 'bb_lower', 'is_volume_spike'])

    print("Successfully inserted 100 mocked candles and indicators into ClickHouse!")
except Exception as e:
    print(f"Failed: {e}")
