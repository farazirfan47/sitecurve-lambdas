import { SQSEvent } from "aws-lambda";
import { ContentParseJob, ParsedContentItem } from "./types";
import AWS from "aws-sdk";

export const mergeContentPrasePingBack = (event: SQSEvent) => {
  let contentPraseJobs: ContentParseJob[] = [];
  event.Records.forEach((record) => {
    const { body } = record;
    const { id, url, page_id } = JSON.parse(body);
    contentPraseJobs.push({ id, url, page_id });
  });
  return contentPraseJobs;
};

export const putSerpIdsInOpenAIQueue = async (
  parsedContent: ParsedContentItem[]
) => {
  console.log("Putting SERP IDs in OpenAI Queue");
  const sqs = new AWS.SQS();
  let serps: any = [];
  parsedContent.forEach((item) => {
    serps.push({
      type: "serp",
      id: item.serp_id
    });
  });
  const params = {
    QueueUrl: process.env.OPENAI_QUEUE || "",
    MessageBody: JSON.stringify(serps),
  };
  await sqs.sendMessage(params).promise();
};
