import { Injectable, Logger, OnModuleInit, Optional } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import Redis from 'ioredis';
import { InjectRedis } from '@nestjs-modules/ioredis';
import { lastValueFrom } from 'rxjs';
import { BinanceAdapter } from 'src/libs/binance/binance.service';
import { CoindeskAdapter } from 'src/libs/coindesk/coindesk.service';
import {
  DEFAULT_INTERVAL,
  DEFAULT_INTERVAL_TIME,
  LIMIT_CANDLE,
} from 'src/utils/constant';
import { promiseLimitVer2 } from 'src/utils/helpers';
import { RedisKey } from 'src/utils/RedisKey';
import { RedisLock } from 'src/utils/redis.utils';
import { getTimestampMsInResolution } from 'src/utils/timeHelper';
import { env } from 'src/config';
import { WorkerKafkaService } from '../kafka/worker-kafka.service';
import {
  KafkaEventType,
  KafkaKline1m,
  KafkaKlines1mEvent,
  KafkaNewsEvent,
  KafkaTopics,
} from '../kafka/schema';

const NEWS_SYNC_WINDOW_SECONDS = 60 * 60;

@Injectable()
export class WorkerService implements OnModuleInit {
  private logger: Logger = new Logger(WorkerService.name);

  constructor(
    @InjectRedis() private readonly redis: Redis,
    @Optional() private readonly workerKafkaService?: WorkerKafkaService,
  ) {}

  async onModuleInit() {}

  @Cron('* * * * *')
  async syncBinanceTickers() {
    try {
      this.logger.log('Start sync Binance tickers');

      const prices = await BinanceAdapter.getFutureTickers();
      const symbols = prices.map((item) => item.symbol);

      await this.redis.set(RedisKey.binance_tickers, symbols.join(','));

      console.log({
        job: 'syncBinanceTickers',
        totalSymbols: symbols.length,
        syncedAt: new Date().toISOString(),
        data: symbols,
      });
    } catch (err) {
      this.logger.error(err.message);
      console.log(err);
    }
  }

  private formatKline(ticker: string, data: any[][]): KafkaKline1m[] {
    return data.map((item) => {
      return {
        ticker,
        openTime: item[0],
        open: item[1],
        high: item[2],
        low: item[3],
        close: item[4],
        volume: item[5],
        closeTime: item[6],
        quoteAssetVolume: item[7],
        numOfTrades: item[8],
        takerBuyBaseAssetVolume: item[9],
        takerBuyQuoteAssetVolume: item[10],
        ignore: item[11],
      };
    });
  }

  private getKlineCheckpointKey(ticker: string) {
    return `${RedisKey.binance_klines_1m_checkpoint}:${ticker}`;
  }

  private getDefaultKlineCheckpoint() {
    return getTimestampMsInResolution(
      Date.now() - 60 * DEFAULT_INTERVAL_TIME,
      DEFAULT_INTERVAL_TIME,
    );
  }

  private async getKlineCheckpoint(ticker: string) {
    const rawCheckpoint = await this.redis.get(
      this.getKlineCheckpointKey(ticker),
    );
    const checkpoint = Number(rawCheckpoint);

    if (Number.isFinite(checkpoint) && checkpoint > 0) {
      return checkpoint;
    }

    return this.getDefaultKlineCheckpoint();
  }

  async getNewKlines1mByTicker(
    ticker: string,
    fromTime: number,
    toTime: number,
  ) {
    const limit = Math.min(
      Math.ceil((toTime - fromTime) / DEFAULT_INTERVAL_TIME) + 1,
      LIMIT_CANDLE,
    );
    const startTime = getTimestampMsInResolution(
      fromTime,
      DEFAULT_INTERVAL_TIME,
    );
    const rawKlines = await BinanceAdapter.getFuturesKlines(
      ticker,
      DEFAULT_INTERVAL,
      startTime,
      limit,
    );

    return this.formatKline(ticker, rawKlines).filter(
      (item) => item.openTime >= startTime && item.openTime <= toTime,
    );
  }

  private async emitKlines1mEvent(event: KafkaKlines1mEvent) {
    if (!env.flag.isRunningKafka || !this.workerKafkaService) {
      return;
    }

    await lastValueFrom(
      this.workerKafkaService.emitMessage(KafkaTopics.klines1m, event),
    );
  }

  private getDefaultNewsCheckpoint() {
    return Math.floor(Date.now() / 1000) - NEWS_SYNC_WINDOW_SECONDS;
  }

  private async getNewsCheckpoint() {
    const rawCheckpoint = await this.redis.get(
      RedisKey.coindesk_news_checkpoint,
    );
    const checkpoint = Number(rawCheckpoint);

    if (Number.isFinite(checkpoint) && checkpoint > 0) {
      return checkpoint;
    }

    return this.getDefaultNewsCheckpoint();
  }

  private async emitNewsEvent(event: KafkaNewsEvent) {
    if (!env.flag.isRunningKafka || !this.workerKafkaService) {
      return;
    }

    await lastValueFrom(
      this.workerKafkaService.emitMessage(KafkaTopics.news, event),
    );
  }

  @Cron('* * * * *')
  async syncKlines1m() {
    const isSetLockSuccessful = await RedisLock.setLock(
      this.redis,
      RedisKey.is_locking_sync_new_kline,
      60,
    );
    if (!isSetLockSuccessful) return;

    try {
      this.logger.log('Start sync klines 1m');

      const rawSymbols = await this.redis.get(RedisKey.binance_tickers);
      const symbols = rawSymbols
        ? rawSymbols.split(',').filter(Boolean)
        : (await BinanceAdapter.getFutureTickers()).map((item) => item.symbol);
      const toTime = getTimestampMsInResolution(
        Date.now(),
        DEFAULT_INTERVAL_TIME,
      );

      const tasks = symbols.map((ticker) => async () => {
        const fromTime = await this.getKlineCheckpoint(ticker);
        if (fromTime >= toTime) {
          return { ticker, totalKlines: 0 };
        }

        const klines = await this.getNewKlines1mByTicker(
          ticker,
          fromTime,
          toTime,
        );
        const nextCheckpoint = klines.length
          ? Math.max(...klines.map((item) => item.openTime)) +
            DEFAULT_INTERVAL_TIME
          : fromTime;

        if (klines.length) {
          const event: KafkaKlines1mEvent = {
            type: KafkaEventType.Klines1mSynced,
            ticker,
            interval: DEFAULT_INTERVAL,
            fromTime,
            toTime,
            nextCheckpoint,
            klines,
            syncedAt: new Date().toISOString(),
          };
          await this.emitKlines1mEvent(event);
          await this.redis.set(
            this.getKlineCheckpointKey(ticker),
            nextCheckpoint,
          );
        }

        return { ticker, totalKlines: klines.length };
      });

      const results = await promiseLimitVer2(tasks, 10);
      const totalKlines = results.reduce(
        (sum, item) => sum + item.totalKlines,
        0,
      );

      console.log({
        job: 'syncKlines1m',
        totalSymbols: symbols.length,
        totalKlines,
        syncedAt: new Date().toISOString(),
      });
    } catch (err) {
      this.logger.error(err.message);
      console.log(err);
    } finally {
      await RedisLock.releaseLock(
        this.redis,
        RedisKey.is_locking_sync_new_kline,
      );
    }
  }

  @Cron('* * * * *')
  async syncNews() {
    const isSetLockSuccessful = await RedisLock.setLock(
      this.redis,
      RedisKey.is_locking_sync_news,
      60,
    );
    if (!isSetLockSuccessful) return;

    try {
      this.logger.log('Start sync news');

      const fromTime = await this.getNewsCheckpoint();
      const now = Math.floor(Date.now() / 1000);
      const toTime = Math.min(fromTime + NEWS_SYNC_WINDOW_SECONDS, now);

      if (fromTime >= toTime) {
        return;
      }

      const news = (await CoindeskAdapter.fetchCoinDeskNews(toTime))
        .filter(
          (item) => item.publishedOn > fromTime && item.publishedOn <= toTime,
        )
        .sort((a, b) => a.publishedOn - b.publishedOn);
      const nextCheckpoint = news.length
        ? Math.max(...news.map((item) => item.publishedOn))
        : toTime;

      if (news.length) {
        const event: KafkaNewsEvent = {
          type: KafkaEventType.NewsSynced,
          fromTime,
          toTime,
          nextCheckpoint,
          news,
          syncedAt: new Date().toISOString(),
        };
        await this.emitNewsEvent(event);
      }

      await this.redis.set(RedisKey.coindesk_news_checkpoint, nextCheckpoint);

      console.log({
        job: 'syncNews',
        fromTime,
        toTime,
        nextCheckpoint,
        totalNews: news.length,
        syncedAt: new Date().toISOString(),
      });
    } catch (err) {
      this.logger.error(err.message);
      console.log(err);
    } finally {
      await RedisLock.releaseLock(this.redis, RedisKey.is_locking_sync_news);
    }
  }
}
