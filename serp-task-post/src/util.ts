import { SQSEvent } from 'aws-lambda';

// This function will merge the keyword ids from the SQS event, lambda will get up to 10 jobs at a time
export const mergedKeywords = (event: SQSEvent) => {
    let keyword_ids: number[] = [];
    event.Records.forEach((record) => {
        const { body } = record;
        // body will give us the array of ids that we need to merge into the main array
        keyword_ids = keyword_ids.concat(JSON.parse(body));
    });
    return keyword_ids;
}