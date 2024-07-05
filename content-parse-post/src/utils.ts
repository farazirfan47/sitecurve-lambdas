import { SQSEvent } from 'aws-lambda';
import { SerpJob } from './types';

export const mergedSerpBatch = (event: SQSEvent) => {
    let serpJobs: SerpJob[] = [];
    event.Records.forEach((record) => {
        const { body } = record;
        const thirtySerps = JSON.parse(body);
        thirtySerps.forEach((singleDomain: SerpJob) => {
            serpJobs.push(singleDomain);
        })
    });
    return serpJobs;
}