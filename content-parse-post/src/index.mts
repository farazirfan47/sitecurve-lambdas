import { createAuthenticatedFetch } from "./dataforseo.js";
import { initMongoClient, updateDomainStatuses } from "./mongodb.js";
import { SerpJob } from "./types.js";
import { mergedSerpBatch } from "./utils.js";
import { SQSEvent } from "aws-lambda";
import * as client from 'dataforseo-client'

export const handler = async (event: SQSEvent) => {
    try{
        console.log('Received event:', JSON.stringify(event, null, 2));
        let mongoClient = await initMongoClient();
        let serpJobs: SerpJob[] = mergedSerpBatch(event);
        let onPageApi = new client.OnPageApi("https://api.dataforseo.com", { fetch: createAuthenticatedFetch() });
        let batchFailed = false;
    
        // Divide serpJobs into chunks of 100 each
        const chunks: SerpJob[][] = [];
        const chunkSize = 100;
        for (let i = 0; i < serpJobs.length; i += chunkSize) {
            chunks.push(serpJobs.slice(i, i + chunkSize));
        }
        
        await Promise.all(chunks.map(async (chunk) => {
            let tasks: any = [];
            chunk.map((serpJob) => {
                let task = new client.OnPageTaskRequestInfo();
                // Remove http, https:// and www from serpJob.url
                task.target = serpJob.url.replace(/(^\w+:|^)\/\//, '').replace('www.', '');
                task.max_crawl_pages = 1;
                task.start_url = serpJob.url;
                task.force_sitewide_checks = true;
                task.enable_content_parsing = true;
                task.support_cookies = true;
                task.disable_cookie_popup = true;
                task.return_despite_timeout = true;
                task.tag = serpJob.url;
                task.pingback_url = "https://2vqsvcldyg.execute-api.us-east-2.amazonaws.com/default/content-parse-postback?id=$id&tag=$tag&page_id=" + serpJob.serp_id;
                tasks.push(task);
            });
            // Now we have 100 tasks loaded into the tasks array
            let resp = await sendOnPageTasks(onPageApi, tasks);
            if(!resp){
                console.log("Content Parse Task Posting Failed");
                console.log(tasks);
                // Update these serp domain statuses in MongoDB
                await updateDomainStatuses(mongoClient, chunk, "FAILED");
            }
        }));

        await mongoClient.close();
        return {
            statusCode: 200,
            body: JSON.stringify('Success'),
        };

    }catch(e){
        console.log(e);
        return {
            statusCode: 500,
            body: JSON.stringify('Internal Server Error'),
        };
    }
};

const sendOnPageTasks = async (onPageApi: client.OnPageApi, tasks: client.OnPageTaskRequestInfo[], rateLimitRetry: number = 0) => {
    try{
        let response = await onPageApi.taskPost(tasks);
        if(response?.status_code == 20000){
            console.log("Tasks sent successfully");
            return true;
        }else if (response?.status_code == 40202){
            // sleep for 5 seconds and retry
            await new Promise(resolve => setTimeout(resolve, 5000));
            if(rateLimitRetry < 15){
                await sendOnPageTasks(onPageApi, tasks, rateLimitRetry + 1);
            }else{
                console.log("Rate limit exceeded.");
                return false;
            }
        }else if (response?.status_code == 40200){
            console.log("Payment Required.");
            return false;
        }
    }catch(e){
        console.log(e);
    }
}