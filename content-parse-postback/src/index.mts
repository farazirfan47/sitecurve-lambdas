import AWS from "aws-sdk";
import { APIGatewayEvent } from "aws-lambda";

type ContentParseJob = {
  id: string;
  url: string;
  page_id: string;
};

export const handler = async (event: APIGatewayEvent) => {
  try {
    console.log("New SERP POSTBACK event");
    console.log("Event: ", event)

    const qs = event.queryStringParameters;

    if (qs) {
      const sqs = new AWS.SQS();
      const params = {
        QueueUrl: process.env.QUEUE_URL || "",
        MessageBody: JSON.stringify({
          id: qs.id,
          url: qs.tag, // Tag is a URL
          page_id: qs.page_id // page_id is a serp_id
        } as ContentParseJob),
      };
      await sqs.sendMessage(params).promise();
      console.log("Pingback URL pushed to SQS queue.");
    }

    const response = {
      statusCode: 200,
      body: JSON.stringify("OK"),
    };
    return response;
  } catch (e) {
    console.error(e);
    return {
      statusCode: 500,
      body: JSON.stringify("Internal Server Error"),
    };
  }
};
