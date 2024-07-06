import { KeywordArray, KeywordRow } from "./types";
import { SQSEvent } from "aws-lambda";
import AWS from "aws-sdk";

// This function will merge the keyword ids from the SQS event, lambda will get up to 10 jobs at a time
export const mergedKeywords = (event: SQSEvent) => {
  let keyword_ids: number[] = [];
  event.Records.forEach((record) => {
    const { body } = record;
    // body will give us the array of ids that we need to merge into the main array
    keyword_ids = keyword_ids.concat(JSON.parse(body));
  });
  return keyword_ids;
};

export const chunkArray = (array: KeywordArray, chunkSize: number) => {
  const result = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    result.push(array.slice(i, i + chunkSize));
  }
  return result;
};

export const groupChunks = (
  chunks: KeywordArray[],
  maxChunksPerGroup: number
) => {
  const groupedChunks = [];
  for (let i = 0; i < chunks.length; i += maxChunksPerGroup) {
    groupedChunks.push(chunks.slice(i, i + maxChunksPerGroup));
  }
  return groupedChunks;
};

export const divideAndGroupValues = (
  values: KeywordArray,
  chunkSize: number,
  groupSize: number
) => {
  const chunks = chunkArray(values, chunkSize);
  const groupedChunks = groupChunks(chunks, groupSize);
  return groupedChunks;
};

export const generateKeywordIds = (
  hundredChunks: KeywordArray[],
  allKeywords: KeywordRow[]
) => {
  let keywordIds: string[] = [];
  let keywordMap: any = {};
  allKeywords.forEach((row) => {
    keywordMap[row.keyword] = row._id;
  });
  hundredChunks.forEach((singleChunk) => {
    singleChunk.forEach((row) => {
      keywordIds.push(keywordMap[row.keyword]);
    });
  });
  return keywordIds;
};

export const generateKeywordIdsForTaskresponse = (
  hundredChunks: string[][],
  allKeywords: KeywordRow[]
) => {
  let keywordIds: string[] = [];
  let keywordMap: any = {};
  allKeywords.forEach((row) => {
    keywordMap[row.keyword] = row._id;
  });
  hundredChunks.forEach((singleChunk) => {
    singleChunk.forEach((row) => {
      keywordIds.push(keywordMap[row]);
    });
  });
  return keywordIds;
};

export const openAIHundredKeywordChunks = (
  singleChunk: KeywordRow[],
  chunkSize: number
) => {
  const chunks = chunkArray(singleChunk, chunkSize);
  return chunks;
};

export const putChunkstoOpenAI = async (chunks: KeywordArray[]) => {
  // 100 keyword in each job
  const sqs = new AWS.SQS();
  for (const chunk of chunks) {
    let keywords: any = [];
    chunk.forEach((item) => {
      keywords.push({
        id: item._id,
        keyword: item.keyword,
        type: "keyword",
      });
    });
    const params = {
      MessageBody: JSON.stringify(keywords),
      QueueUrl: process.env.OPENAI_QUEUE || "",
    };
    await sqs.sendMessage(params).promise();
    console.log("Chunk sent to OpenAI");
  }
};
