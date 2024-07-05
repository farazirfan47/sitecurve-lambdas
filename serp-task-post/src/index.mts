import { createAuthenticatedFetch } from "./dataforseo.js";
import { mergedKeywords } from "./util.js";
import * as client from "dataforseo-client";
import { SQSEvent } from "aws-lambda";
import { getKeywords, initMongoClient, updateKeywordsStatus } from "./mongodb.js";

type KeywordRow = {
  id: string;
  keyword: string;
};

type KeywordArray = KeywordRow[];

export const handler = async (event: SQSEvent) => {
  try {
    console.log("Received event:", JSON.stringify(event, null, 2));

    let keyword_ids: number[] = mergedKeywords(event);
    // let ch_client = await initClickHouseClient();
    let mongoClient = await initMongoClient();
    let serpApi = new client.SerpApi("https://api.dataforseo.com", {
      fetch: createAuthenticatedFetch(),
    });

    // Fetch keyword data from Clickhouse
    // const rows: KeywordRow[]  = await getKeywords(ch_client, keyword_ids);
    const rows: KeywordRow[] = await getKeywords(mongoClient, keyword_ids);

    // Divide rows into chunks of 100 each
    const chunks: KeywordArray[] = [];
    const chunkSize = 100;
    for (let i = 0; i < rows.length; i += chunkSize) {
      chunks.push(rows.slice(i, i + chunkSize));
    }

    await Promise.all(
      chunks.map(async (chunk) => {
        let tasks: client.SerpTaskRequestInfo[] = [];
        chunk.forEach(async (row) => {
          let task = new client.SerpTaskRequestInfo();
          task.location_code = 2840;
          task.language_code = "en";
          task.keyword = row.keyword;
          task.priority = 2;
          task.depth = 30;
          task.device = "desktop";
          task.tag = row.id;
          task.postback_data = "https://0bmol4utu1.execute-api.us-east-2.amazonaws.com/default/serp-postback";
          tasks.push(task);
        });

        let resp = await sendSerpTasks(serpApi, tasks);
        if (!resp) {
          // Log Task data for debugging later
          console.log("Serp Post API Failed");
          console.log("Task data: ", tasks);
          // Update these tasks status in MongoDB collection keywords with status FAILED
          await updateKeywordsStatus(mongoClient, chunk, "FAILED");
        }
      })
    );

    // Close the mongo client
    await mongoClient.close();

    return {
      statusCode: 200,
      body: JSON.stringify("Batch completed"),
    };

  } catch (e) {
    console.log(e);
    return {
      statusCode: 500,
      body: JSON.stringify("Batch failed"),
    };
  }
};

const sendSerpTasks = async (
  serpApi: client.SerpApi,
  tasks: client.SerpTaskRequestInfo[],
  rateLimitRetry = 0
) => {
  try {
    let response = await serpApi.googleOrganicTaskPost(tasks);
    if (response?.status_code == 20000) {
      console.log("Tasks sent successfully");
      return true;
    } else if (response?.status_code == 40202) {
      // sleep for 5 seconds and retry
      await new Promise((resolve) => setTimeout(resolve, 5000));
      if (rateLimitRetry < 15) {
        await sendSerpTasks(serpApi, tasks, rateLimitRetry + 1);
      } else {
        console.log("Rate limit exceeded.");
        return false;
      }
    } else if (response?.status_code == 40200) {
      console.log("Payment Required.");
      return false;
    }
  } catch (e) {
    console.log(e);
  }
};
