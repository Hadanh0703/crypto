import axios from 'axios';
import { env } from 'src/config';
import { KafkaNewsItem } from 'src/modules/kafka/schema';

export namespace CoindeskAdapter {
  let index = 0;

  export function getApiKey() {
    if (!env.coindesk.apiKeys.length) {
      return undefined;
    }

    const apiKey = env.coindesk.apiKeys[index];
    index = (index + 1) % env.coindesk.apiKeys.length;
    return apiKey;
  }

  export async function fetchCoinDeskNews(
    toTime: number,
    limit: number = 100,
  ): Promise<KafkaNewsItem[]> {
    const response = await axios.get(
      'https://data-api.coindesk.com/news/v1/article/list',
      {
        params: {
          lang: 'EN',
          limit,
          to_ts: toTime,
          api_key: getApiKey(),
        },
        headers: {
          'Content-Type': 'application/json; charset=UTF-8',
        },
      },
    );

    return (response.data.Data ?? []).map((item) => {
      return {
        id: item.ID,
        type: item.TYPE,
        guid: item.GUID,
        url: item.URL,
        publishedOn: item.PUBLISHED_ON,
        imageUrl: item.IMAGE_URL,
        title: item.TITLE,
        subtitle: item.SUBTITLE,
        authors: item.AUTHORS,
        sourceId: item.SOURCE_ID,
        rawBody: item.BODY,
        keywords: item.KEYWORDS,
        sentiment: item.SENTIMENT,
        sourceName: item.SOURCE_DATA?.NAME,
        categories: (item.CATEGORY_DATA ?? []).map((item) => item.CATEGORY),
      };
    });
  }
}
