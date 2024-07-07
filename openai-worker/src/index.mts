import { SQSEvent } from "aws-lambda";
import {
  generateKeywordTagPrompt,
  generateSerpPrompt,
  mergeBatchData,
  saveTaggedKeywords,
  tagSerpDomains,
} from "./utils.js";
import OpenAI from "openai";
import { fetchSerpsFromDb, initMongoClient } from "./mongodb.js";
import { DomainResult, KeywordResult } from "./types.js";
import { backOff } from "exponential-backoff";

export const handler = async (event: SQSEvent) => {
  try {
    // We can have max of bacth 2
    // 2x50 = 100
    // 20 worker instances are working that means 100x20 = 2000 simultaneous requets to OpenAI
    console.log("Received event:", JSON.stringify(event, null, 2));
    const openai = new OpenAI();
    const mongoClient = await initMongoClient();
    let mergedData = mergeBatchData(event.Records);
    console.log("Merged data", mergedData);

    let serpIds: string[] = [];
    let dbSerps: any = [];
    let combineJobs = [];
    if (mergedData.serp) {
      serpIds = mergedData.serp.map((item) => item.id);
      dbSerps = await fetchSerpsFromDb(mongoClient, serpIds);
      console.log("Fetched serps from db", dbSerps);
    }
    // combine result will have db serps and keyword in a single array so that we can send parallel requests to openai, combine the dbSerps and mergedData.keyword arrays
    if (mergedData.keyword) {
      combineJobs = dbSerps.concat(mergedData.keyword);
    } else {
      combineJobs = dbSerps;
    }

    console.log("Combine jobs", combineJobs);

    let domainResults: DomainResult[] = [];
    let keywordResults: KeywordResult[] = [];

    // await Promise.all(
    for (let job of combineJobs) {
      // combineJobs.map(async (job: any) => {
      if (job?.type && job?.type === "keyword") {
        try {
          let result: KeywordResult | undefined = await backOff(
            async () => await tagKeyword(openai, job),
            {
              numOfAttempts: 5,
              startingDelay: 3000,
              timeMultiple: 2,
            }
          );
          if (result) {
            keywordResults.push(result);
          }
        } catch (e) {
          console.log("Error in tagging keyword", e);
        }
      } else {
        if (job.content_parse_status != "NO_CONTENT" && job.page_meta) {
          try {
            let result: DomainResult | undefined = await backOff(
              async () => await tagSerp(openai, job),
              {
                numOfAttempts: 5,
                startingDelay: 3000,
                timeMultiple: 2,
              }
            );
            if (result) {
              domainResults.push(result);
            }
          } catch (e) {
            console.log("Error in tagging serp", e);
          }
        }
      }
      //})
      //);
    }
    // Now we have domainResults and we will update the serps in the mongoDB in bulk
    if (domainResults.length > 0) {
      await tagSerpDomains(mongoClient, domainResults);
    }
    if (keywordResults.length > 0) {
      await saveTaggedKeywords(mongoClient, keywordResults);
    }

    console.log("Completed tagging");
    await mongoClient.close();
    return {
      statusCode: 200,
      body: JSON.stringify("Completed tagging"),
    };
  } catch (e) {
    console.log(e);
    return {
      statusCode: 500,
      body: JSON.stringify("Error in tagging"),
    };
  }
};

const tagKeyword = async (openai: OpenAI, job: any) => {
  let prompt = generateKeywordTagPrompt(job);
  console.log("Prompt", prompt);
  const response = await openai.chat.completions.create({
    model: "gpt-4-turbo",
    messages: [
      {
        role: "system",
        content: "You are a tagging expert that will analyse keywords and web pages metadata and tag them with category and niche.",
      },
      {
        role: "user",
        content: prompt,
      },
    ],
    temperature: 1,
    max_tokens: 500,
    top_p: 1,
    frequency_penalty: 0,
    presence_penalty: 0,
  });
  console.log("Response", response);
  // Convert the response to JSON
  if(response.choices[0].message.content){
    let res = JSON.parse(response.choices[0].message.content);
    let result: KeywordResult = { _id: job.id, res };
    return result;
  }
};

const tagSerp = async (openai: OpenAI, job: any) => {
  let prompt = generateSerpPrompt(job.page_meta);
  console.log("Prompt", prompt);
  const response = await openai.chat.completions.create({
    model: "gpt-4-turbo",
    messages: [
      {
        role: "system",
        content: "You are a tagging expert that will analyse keywords and web pages metadata and tag them with website types and business model.",
      },
      {
        role: "user",
        content: prompt,
      },
    ],
    temperature: 1,
    max_tokens: 500,
    top_p: 1,
    frequency_penalty: 0,
    presence_penalty: 0,
  });
  console.log("Response", response);
  // Convert the response to JSON
  if(response.choices[0].message.content){
    let res = JSON.parse(response.choices[0].message.content);
    let result: DomainResult = { _id: job._id, res };
    return result;
  }
};
