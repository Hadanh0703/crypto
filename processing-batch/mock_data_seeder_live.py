import clickhouse_connect
import random
import time

try:
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

    ticker = 'BTCUSDT'
    interval = 300000
    
    # Generate data for the last 300 intervals (approx 24 hours) ending NOW
    end_time = int(time.time() * 1000)
    # Align to 5m bucket
    end_time = end_time - (end_time % interval)
    
    start_time = end_time - (300 * interval)

    print("Generating LIVE mock data...")
    fact_records = []
    ind_records = []

    base_price = 68000.0

    for i in range(300):
        open_time = start_time + (i * interval)
        base_price += (random.random() - 0.48) * 100
        
        op = base_price
        hi = base_price + 80
        lo = base_price - 80
        cl = base_price + (random.random() - 0.5) * 40
        vol = 2000.0 + random.random() * 1000
        
        ma20 = cl * 1.002
        ma50 = cl * 0.998
        rsi = 30.0 + random.random() * 40
        bb_upper = cl * 1.03
        bb_lower = cl * 0.97
        
        fact_records.append([ticker, open_time, op, hi, lo, cl, vol, open_time + interval - 1, 0.0, 0, 0.0, 0.0, 0.0])
        ind_records.append([ticker, open_time, ma20, ma50, rsi, bb_upper, bb_lower, 0])

    print("Inserting live facts...")
    client.insert('future_kline_5m', fact_records, column_names=['ticker', 'openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'])

    print("Inserting live indicators...")
    client.insert('kline_indicators', ind_records, column_names=['ticker', 'openTime', 'ma20', 'ma50', 'rsi', 'bb_upper', 'bb_lower', 'is_volume_spike'])

    print("Successfully inserted 300 LIVE mocked candles and indicators into ClickHouse!")
except Exception as e:
    print(f"Failed: {e}")
