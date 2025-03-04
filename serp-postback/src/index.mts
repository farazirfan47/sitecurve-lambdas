
// We will receive postback data for each we set in SERP POST request
// Every task will have 30 results against a keyword
// We will save 30 results in the clickhouse
// We will also send 30 results in a OPEN AI Queue for further processing and tagging

import { APIGatewayEvent } from "aws-lambda";
import * as zlib from "zlib";
import { parseTags, sendToContentPraseQueue } from "./utils.js";
import { ResultItem, SerpRow } from "./types.js";
import { initMongoClient, saveInMongoDB } from "./mongodb.js";
import { ObjectId } from "mongodb";

export const handler = async (event: APIGatewayEvent) => {
  try {
    console.log("New SERP POSTBACK event");
    console.log("Received event:", event);

    let mongoClient = await initMongoClient();
    // Extract the parts of the URL from the event object
    let serps: SerpRow[] = [];

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
        console.log("Parsed body: ", JSON.stringify(parsedBody));

        if (parsedBody.status_code == 20000) {
          if (parsedBody.tasks_count > 0) {
            // We have some tasks
            for (let task of parsedBody.tasks) {
              // Tag has multiple values id=1234&ls_id=456 we will split them and save into their own variables
              let tags = parseTags(task.data.tag);
              if (task?.result_count > 0) {
                let result = task?.result[0];
                result.items.forEach((item: ResultItem) => {
                  let serp: SerpRow = {
                    // Generate random mongo id for the record
                    _id: new ObjectId(),
                    landscape_id: tags.ls_id,
                    keyword_id: tags.id,
                    rank_group: item.rank_group,
                    rank_absolute: item.rank_absolute,
                    title: item.title,
                    description: item.description,
                    url: item.url,
                    breadcrumb: item.breadcrumb,
                  };
                  serps.push(serp);
                });
                // At this point we had 30 SERP rows ready in the serps array
                // Save into ClickHouse
                // let savedSerps = await saveSerps(ch_client, serps, tags.id);
                // Save into MongoDB
                let savedSerps = await saveInMongoDB(mongoClient, serps, tags.id);
                console.log("Saved SERP rows in MongoDB: ", savedSerps);
                // Send to OpenAI Queue
                await sendToContentPraseQueue(savedSerps);
                console.log("Sent SERP rows to Content Parse Queue");
              }
            }
          }
        }
      }
    }

    await mongoClient.close();
    const response = {
      statusCode: 200,
      body: JSON.stringify("Serp Postback Succeeded!"),
    };
    return response;
  } catch (e) {
    console.log("Error in SERP POSTBACK");
    console.error(e);
    return {
      statusCode: 500,
      body: JSON.stringify(e),
    };
  }
};
