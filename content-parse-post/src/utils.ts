import { SQSEvent } from 'aws-lambda';
import { SerpJob } from './types';

export const mergedSerpBatch = (event: SQSEvent) => {
    let serpJobs: SerpJob[] = [];
    event.Records.forEach((record) => {
        const { body } = record;
        const { type, url, serp_id } = JSON.parse(body);
        serpJobs.push({ type, url, serp_id });
    });
    return serpJobs;
}