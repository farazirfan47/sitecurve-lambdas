import { ClickHouseClient, createClient } from "@clickhouse/client";
import * as client from 'dataforseo-client'

export const initClickHouseClient = async (): Promise<ClickHouseClient> => {
    const client = createClient({
      host: 'https://FQDN.aws.clickhouse.cloud',
      username: 'default',
      password: 'password',
      database: 'default',
      application: `pingpong`,
    });
  
    console.log('ClickHouse ping');
    if (!(await client.ping())) {
      throw new Error('failed to ping clickhouse!');
    }
    console.log('ClickHouse pong!');
    return client;
};

export const getKeywords = async (client: ClickHouseClient, keyword_ids: number[]): Promise<any> => {
    const result = await client.query({
        query: `SELECT id, keyword FROM keywords WHERE id IN (${keyword_ids.join(',')})`,
        format: 'JSONEachRow',
    });
    return result.json();
}