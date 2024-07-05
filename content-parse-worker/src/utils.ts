import { SQSEvent } from "aws-lambda";
import { ContentParseJob, ParsedContentItem } from "./types";
import AWS from "aws-sdk";

export const mergeContentPrasePingBack = (event: SQSEvent) => {
  let contentPraseJobs: ContentParseJob[] = [];
  event.Records.forEach((record) => {
    const { body } = record;
    const { id, url } = JSON.parse(body);
    contentPraseJobs.push({ id, url });
  });
  return contentPraseJobs;
};

export const putSerpIdsInOpenAIQueue = async (
  parsedContent: ParsedContentItem[]
) => {
  const sqs = new AWS.SQS();
  let serps: any = [];
  parsedContent.forEach(async (item) => {
    serps.push({
      type: "serp",
      id: item.serp_id
    });
  });
  const params = {
    QueueUrl: process.env.OPENAI_QUEUE_URL || "",
    MessageBody: JSON.stringify(serps),
  };
  await sqs.sendMessage(params).promise();
};
