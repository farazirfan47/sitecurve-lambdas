import AWS from "aws-sdk";
import { SerpTagJob } from "./types";

type Tag = {
  [key: string]: string;
};

export const parseTags = (tag: string): Tag => {
  const tagParts = tag.split("&");
  const tagObj: Tag = {};
  for (let part of tagParts) {
    const [key, value] = part.split("=");
    tagObj[key] = value;
  }
  return tagObj;
};

export const sendToContentPraseQueue = async (serps: any) => {
  let serpJobs: SerpTagJob[] = [];
  for (let serp of serps) {
    serpJobs.push({
      type: "serp",
      url: serp.url,
      serp_id: serp.id,
    });
  }
  const sqs = new AWS.SQS();
  const params = {
    QueueUrl: process.env.CONTENT_PARSE_QUEUE_URL || "",
    MessageBody: JSON.stringify(serpJobs)
  };
  return await sqs.sendMessage(params).promise();
}