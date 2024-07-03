import { APIGatewayEvent } from "aws-lambda";
import { initClickHouseClient } from "./clickhouse.js";
import { createAuthenticatedFetch } from "./dataforseo.js";
import * as client from 'dataforseo-client'

export const handler = async (event: APIGatewayEvent) => {

  console.log("New SERP POSTBACK event");
  const qs = event.queryStringParameters;
  let ch_client = await initClickHouseClient();
  let onPageApi = new client.OnPageApi("https://api.dataforseo.com", { fetch: createAuthenticatedFetch() });

  if(qs){
    let task = new client.OnPageContentParsingRequestInfo();
    // task.url = qs.url;
    task.id = qs.id;
    let resp = await onPageApi.contentParsing([task]);
  }
  const response = {
    statusCode: 200,
    body: JSON.stringify("Hello from Lambda!"),
  };
  return response;
};
