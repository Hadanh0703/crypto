import axios from 'axios';

export namespace BinanceAdapter {
  export const getFutureTickers = async () => {
    const res = await axios.get(
      'https://fapi.binance.com/fapi/v2/ticker/price',
    );
    return res.data as { symbol: string; price: string; time?: number }[];
  };

  export const getFuturesKlines = async (
    ticker: string,
    interval: string,
    startTime: number,
    limit: number,
  ) => {
    try {
      const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
        params: {
          symbol: ticker,
          interval,
          startTime,
          limit,
        },
      });

      return res.data as any[][];
    } catch (err) {
      console.log(err);
      return [];
    }
  };
}
