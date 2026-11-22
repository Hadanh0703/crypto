import { env } from 'src/config';

export const KafkaTopics = {
  ingestionEvents: `${env.kafka.topicPrefix}-ingestion-collector-events`,
  binanceTickers: `${env.kafka.topicPrefix}-ingestion-collector-binance-tickers`,
  klines1m: `${env.kafka.topicPrefix}-ingestion-collector-klines-1m`,
  news: `${env.kafka.topicPrefix}-ingestion-collector-news`,
};

export enum KafkaEventType {
  BinanceTickersSynced = 'BinanceTickersSynced',
  Klines1mSynced = 'Klines1mSynced',
  NewsSynced = 'NewsSynced',
}

export class KafkaBinanceTickersEvent {
  type: KafkaEventType.BinanceTickersSynced;
  symbols: string[];
  syncedAt: string;
}

export type KafkaKline1m = {
  ticker: string;
  openTime: number;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  closeTime: number;
  quoteAssetVolume: string;
  numOfTrades: number;
  takerBuyBaseAssetVolume: string;
  takerBuyQuoteAssetVolume: string;
  ignore: string;
};

export class KafkaKlines1mEvent {
  type: KafkaEventType.Klines1mSynced;
  ticker: string;
  interval: '1m';
  fromTime: number;
  toTime: number;
  nextCheckpoint: number;
  klines: KafkaKline1m[];
  syncedAt: string;
}

export type KafkaNewsItem = {
  id: number;
  type: string;
  guid: string;
  url: string;
  publishedOn: number;
  imageUrl: string;
  title: string;
  subtitle: string;
  authors: string;
  sourceId: string;
  rawBody: string;
  keywords: string;
  sentiment: string;
  sourceName: string;
  categories: string[];
};

export class KafkaNewsEvent {
  type: KafkaEventType.NewsSynced;
  fromTime: number;
  toTime: number;
  nextCheckpoint: number;
  news: KafkaNewsItem[];
  syncedAt: string;
}
