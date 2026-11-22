export enum RedisKey {
  flag = 'flag',

  binance_tickers = 'binance_tickers',
  binance_klines_1m_checkpoint = 'binance_klines_1m_checkpoint',
  coindesk_news_checkpoint = 'coindesk_news_checkpoint',

  is_locking_sync_new_kline = 'is_locking_sync_new_kline',
  is_locking_sync_old_kline = 'is_locking_sync_old_kline',
  is_locking_sync_news = 'is_locking_sync_news',
}
