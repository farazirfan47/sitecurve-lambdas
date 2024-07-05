import { SQSEvent } from "aws-lambda";
import { ContentParseJob, ParsedContentItem } from "./types.js";
import { mergeContentPrasePingBack, putSerpIdsInOpenAIQueue } from "./utils.js";
import * as client from "dataforseo-client";
import { createAuthenticatedFetch } from "./dataforseo.js";
import { initMongoClient, updateDomainStatuses } from "./mongodb.js";

export const handler = async (event: SQSEvent) => {
  console.log("Received event:", JSON.stringify(event, null, 2));
  let mongoClient = await initMongoClient();
  let cpPingBacks: ContentParseJob[] = mergeContentPrasePingBack(event);
  let onPageApi = new client.OnPageApi("https://api.dataforseo.com", {
    fetch: createAuthenticatedFetch(),
  });

  // We got up to 100 pingbacks as we could get 100 messages in a batch from SQS
  // Divide cpPingBacks into chunks of 50 each

  const chunks: ContentParseJob[][] = [];
  const chunkSize = 50;
  for (let i = 0; i < cpPingBacks.length; i += chunkSize) {
    chunks.push(cpPingBacks.slice(i, i + chunkSize));
  }

  // Loop through each chunk
  await Promise.all(
    chunks.map(async (chunk, i) => {
      let parsedContent: ParsedContentItem[] = [];
      let tasks: client.OnPagePagesRequestInfo[] = [];
      chunk.map((cpPingBack) => {
        let task = new client.OnPagePagesRequestInfo();
        task.id = cpPingBack.id;
        task.limit = 1;
        // task.url = cpPingBack.url;
        task.filters = [
          ["resource_type","=","html"]
        ];
        tasks.push(task);
      });
      // Now we have 50 tasks loaded into the tasks array
      // let resp = await sendContentParsingApi(onPageApi, tasks);
      let resp = await sendOnPagePageApi(onPageApi, tasks);
      if (resp) {
        resp.tasks?.forEach((task) => {
          if (task?.status_code == 20000) {
            if (task.result_count && task.result_count > 0) {
              let result = task.result ? task.result[0] : null;
              if (result && result.items_count && result.items_count > 0) {
                // Fetch Serp ID from task data that will help update data in ClickHouse
                let pingpackUrl = task.data ? task.data.pingback_url : null;
                // Get query string vars from pingback URL
                let url = new URL(pingpackUrl);
                let serpId = url.searchParams.get("page_id");
                if (serpId) {
                  let item = result.items ? result.items[0] : null;
                  if (item?.status_code == 20000) {
                    parsedContent.push({
                      serp_id: serpId,
                      page_meta: item.meta,
                    });
                  }
                } else {
                  console.log("Failed to get serp_id from task data");
                }
              }
            }
          } else {
            // Console the error in detail for that specific task
            console.log("Failed to get task result");
            console.log(task);
          }
        });

        // Update these serp domain statuses in MongoDB
        await updateDomainStatuses(mongoClient, parsedContent, "DONE");
        // Put these serp ids in the OpenAI Queue for tragging
        // Parsed content will have 50 page contents
        await putSerpIdsInOpenAIQueue(parsedContent);
        
      } else {
        console.log("Failed to send tasks to API. Exiting loop.");
        console.log("Failed chunk: ", i);
        console.log("Response: ", resp);
      }
    })
  );

  // Close the MongoDB connection
  await mongoClient.close();
  return {
    statusCode: 200,
    body: JSON.stringify("Content parsing completed successfully"),
  };
};


const sendOnPagePageApi = async (
  onPageApi: client.OnPageApi,
  tasks: client.OnPagePagesRequestInfo[],
  rateLimitRetry: number = 0
) => {
  try {
    let response = await onPageApi.pages(tasks);
    if (response?.status_code == 20000) {
      console.log("Tasks sent successfully");
      return response;
    } else if (response?.status_code == 40202) {
      // sleep for 5 seconds and retry
      await new Promise((resolve) => setTimeout(resolve, 5000));
      if (rateLimitRetry < 15) {
        await sendOnPagePageApi(onPageApi, tasks, rateLimitRetry + 1);
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

// const sendContentParsingApi = async (
//   onPageApi: client.OnPageApi,
//   tasks: client.OnPageTaskRequestInfo[],
//   rateLimitRetry: number = 0
// ) => {
//   try {
//     let response = await onPageApi.contentParsing(tasks);
//     if (response?.status_code == 20000) {
//       console.log("Tasks sent successfully");
//       return response;
//     } else if (response?.status_code == 40202) {
//       // sleep for 5 seconds and retry
//       await new Promise((resolve) => setTimeout(resolve, 5000));
//       if (rateLimitRetry < 15) {
//         await sendContentParsingApi(onPageApi, tasks, rateLimitRetry + 1);
//       } else {
//         console.log("Rate limit exceeded.");
//         return false;
//       }
//     } else if (response?.status_code == 40200) {
//       console.log("Payment Required.");
//       return false;
//     }
//   } catch (e) {
//     console.log(e);
//   }
// };
