import { ClickHouseClient, createClient } from "@clickhouse/client";

export const initClickHouseClient = async (): Promise<ClickHouseClient> => {
    const client = createClient({
      url: process.env.CH_HOST,
      username: process.env.CH_USER,
      password: process.env.CH_PASSWORD
    });
  
    console.log('ClickHouse ping');
    if (!(await client.ping())) {
      throw new Error('failed to ping clickhouse!');
    }
    console.log('ClickHouse pong!');
    return client;
};