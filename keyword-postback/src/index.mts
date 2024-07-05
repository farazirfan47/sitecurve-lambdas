import { APIGatewayEvent } from "aws-lambda";
import { initMongoClient, saveInMongoDB } from "./mongodb.js";
import * as zlib from "zlib";
import { KeywordResult } from "./types.js";

export const handler = async (event: APIGatewayEvent) => {
  try {
    console.log("New Keyword POSTBACK event");
    console.log(event.body);

    let mongoClient = await initMongoClient();

    const contentEncoding =
      event.headers["Content-Encoding"] || event.headers["content-encoding"];

    if (contentEncoding == "gzip") {
      if (event.body) {
        // Decode the base64-encoded gzip body
        const compressedBody = Buffer.from(event.body, "base64");
        const decompressedBody = zlib
          .gunzipSync(compressedBody)
          .toString("utf-8");
        // Parse the decompressed JSON body
        const parsedBody = JSON.parse(decompressedBody);

        if (parsedBody.status_code == 20000) {
          if (parsedBody.tasks_count > 0) {
            await Promise.all(
              parsedBody.tasks.map(async (task: any) => {
                let keywordResults: KeywordResult[] = [];
                if (task?.result_count > 0) {
                  // Every task will have 500 keywords result that we set in the post request
                  task.result.items.forEach((result: any) => {
                    let keywordResult: KeywordResult = {
                      search_volume: result.search_volume,
                      cpc: result.cpc,
                      competition: result.competition,
                      low_top_of_page_bid: result.low_top_of_page_bid,
                      high_top_of_page_bid: result.high_top_of_page_bid,
                      keyword_api_status: "DONE",
                      monthly_search_volume: result.monthly_searches,
                      keyword: result.keyword,
                    };
                    keywordResults.push(keywordResult);
                  });
                }
                // Now we have 500 keyword results
                // Save into MongoDB
                await saveInMongoDB(mongoClient, keywordResults, task.id);
              })
            );
          }
        }
      }
    }
    const response = {
      statusCode: 200,
      body: JSON.stringify("OK"),
    };
    return response;
  } catch (e) {
    console.log("Error in keyword postback", e);
    const response = {
      statusCode: 500,
      body: JSON.stringify("Error"),
    };
  }
};
