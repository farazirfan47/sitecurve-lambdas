import { ClickHouseClient, createClient } from "@clickhouse/client";
import { SerpRow } from "./types";

export const initClickHouseClient = async (): Promise<ClickHouseClient> => {
    const client = createClient({
      host: process.env.CH_HOST,
      username: process.env.CH_USER,
      password: process.env.CH_PASSWORD,
    });
  
    console.log('ClickHouse ping');
    if (!(await client.ping())) {
      throw new Error('failed to ping clickhouse!');
    }
    console.log('ClickHouse pong!');
    return client;
};

export const saveSerps = async (client: ClickHouseClient, serps: SerpRow[], keyword_id: string) => {
    await client.insert({
      table: 'serps',
      values: serps,
      format: 'JSONEachRow'
    })
    const rows = await client.query({
      query: `SELECT * FROM serps where keyword_id = '${keyword_id}'`,
      format: 'JSONEachRow',
    })
    return await rows.json();
}