import { createAuthenticatedFetch } from "./dataforseo.js";
import {
  divideAndGroupValues,
  generateKeywordIds,
  generateKeywordIdsForTaskresponse,
  mergedKeywords,
  openAIHundredKeywordChunks,
  putChunkstoOpenAI,
} from "./util.js";
import * as client from "dataforseo-client";
import { SQSEvent } from "aws-lambda";
import {
  addTaskIdToKeywords,
  getKeywords,
  initMongoClient,
  updateKeywordsStatus,
} from "./mongodb.js";
import { KeywordRow } from "./types.js";

export const handler = async (event: SQSEvent) => {
  try {

    console.log("Received event:", event);

    let keyword_ids: number[] = mergedKeywords(event);
    console.log("Keyword IDs: ", keyword_ids);

    let mongoClient = await initMongoClient();

    let keywordDataApi = new client.KeywordsDataApi(
      "https://api.dataforseo.com",
      { fetch: createAuthenticatedFetch() }
    );

    // Fetch keyword data from Clickhouse
    // const result = await getKeywords(ch_client, keyword_ids);
    // const rows: KeywordRow[] = await result.json();
    const rows: KeywordRow[] = await getKeywords(mongoClient, keyword_ids);
    console.log("Mongo DB Keyword Rows: ", rows);
    // Divide single chunk into chunks of 100 each
    let hundredKeywordChunks = openAIHundredKeywordChunks(rows, 10);
    console.log("Hundred Keyword Chunks: ", hundredKeywordChunks);
    await putChunkstoOpenAI(hundredKeywordChunks);
    // Chunks where each array will have 100 chunks and each chunk will have 500 keywords
    let chunkGroups = divideAndGroupValues(rows, 200, 50);
    console.log("Chunk Groups: ", chunkGroups);

    await Promise.all(
      chunkGroups.map(async (hundredChunks) => {
        // We got 100 chunks where chunk will have 500 keywords
        let tasks: client.KeywordsDataTaskRequestInfo[] = [];
        for(let singleChunk of hundredChunks) {
          let keys: string[] = singleChunk.map((row) => row.keyword);
          let task = new client.KeywordsDataTaskRequestInfo();
          task.location_code = 2840; // USA
          task.language_code = "en";
          task.keywords = keys;
          task.postback_url = "https://uo1apvg9ib.execute-api.us-east-2.amazonaws.com/default/keyword-postback";
          tasks.push(task);
          // Every task will have 500 keywords
        }
        let resp = await sendKeywordDataTasks(keywordDataApi, tasks);
        if (!resp) {
          console.log("Task Failed");
          // All Tasks have failed means 100x1000 = 10000 keywords
          let keywordIds = generateKeywordIds(hundredChunks, rows);
          await updateKeywordsStatus(mongoClient, keywordIds, "FAILED");
        } else {
          if(resp?.tasks){
            for(let task of resp.tasks) {
              if (task?.status_code == 20100) {
                console.log("Inisde 20100");
                let keywordIds = generateKeywordIdsForTaskresponse([task?.data?.keywords], rows);
                console.log("Keyword IDs: ", keywordIds);
                if (task.id) {
                  await addTaskIdToKeywords(mongoClient, keywordIds, task.id);
                  console.log("Task ID added to keywords");
                }
              }
            };
          }
        }
      })
    );
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

const sendKeywordDataTasks = async (
  keywordDataApi: client.KeywordsDataApi,
  tasks: client.SerpTaskRequestInfo[],
  rateLimitRetry = 0
) => {
  try {
    let response = await keywordDataApi.googleAdsSearchVolumeTaskPost(tasks);
    if (response?.status_code == 20000) {
      console.log("Tasks sent successfully");
      console.log(response);
      return response;
    } else if (response?.status_code == 40202) {
      // sleep for 5 seconds and retry
      await new Promise((resolve) => setTimeout(resolve, 5000));
      if (rateLimitRetry < 15) {
        await sendKeywordDataTasks(keywordDataApi, tasks, rateLimitRetry + 1);
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
    return false;
  }
};
