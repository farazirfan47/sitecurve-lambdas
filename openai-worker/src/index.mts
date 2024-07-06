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

    await Promise.all(
      combineJobs.map(async (job: any) => {
        if (job?.type && job?.type === "keyword") {
          let prompt = generateKeywordTagPrompt(job);
          console.log("Prompt", prompt);
          const response = await openai.completions.create({
            model: "gpt-3.5-turbo-instruct",
            prompt: prompt,
            temperature: 1,
            max_tokens: 500,
            top_p: 1,
            frequency_penalty: 0,
            presence_penalty: 0,
          });
          console.log("Response", response);
          // Convert the response to JSON
          let res = JSON.parse(response.choices[0].text);
          keywordResults.push({ _id: job.id, res });
        } else {
          if (job.content_parse_status != "NO_CONTENT" && job.page_meta) {
            let prompt = generateSerpPrompt(job.page_meta);
            console.log("Prompt", prompt);
            const response = await openai.completions.create({
              model: "gpt-3.5-turbo-instruct",
              prompt: prompt,
              temperature: 1,
              max_tokens: 500,
              top_p: 1,
              frequency_penalty: 0,
              presence_penalty: 0,
            });
            console.log("Response", response);
            // Convert the response to JSON
            let res = JSON.parse(response.choices[0].text);
            domainResults.push({ _id: job._id, res });
          }
        }
      })
    );
    // Now we have domainResults and we will update the serps in the mongoDB in bulk
    if (domainResults.length > 0) {
      await tagSerpDomains(mongoClient, domainResults);
    }
    if (keywordResults.length > 0) {
      await saveTaggedKeywords(mongoClient, keywordResults);
    }

    console.log("Completed tagging");
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
