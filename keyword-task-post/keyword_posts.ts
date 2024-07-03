import { getKeywords, initClickHouseClient, updateByKeywordIds } from './clickhouse';
import { createAuthenticatedFetch } from './dataforseo';
import { mergedKeywords } from './util';
import * as client from 'dataforseo-client'

type KeywordRow = {
    id: string;
    keyword: string;
}

type KeywordArray = KeywordRow[];

export const handler = async (event) => {
    try{
        let keyword_ids: number[] = mergedKeywords(event);
        let ch_client = await initClickHouseClient();
        let batchFailed = false;
    
        let keywordDataApi = new client.KeywordsDataApi("https://api.dataforseo.com", { fetch: createAuthenticatedFetch() });
        
        // Fetch keyword data from Clickhouse
        const result = await getKeywords(ch_client, keyword_ids);
        const rows: KeywordRow[] = await result.json();
        // Create a copy of the rows and arrange in a way where keyword will be the key and id will be the value
        let keywordMap: any = {};
        rows.forEach(row => {
            keywordMap[row.keyword] = row.id;
        });
    
        // Divide rows into chunks of 100 each
        const chunks: KeywordArray[] = [];
        const chunkSize = 100;
        for (let i = 0; i < rows.length; i += chunkSize) {
            chunks.push(rows.slice(i, i + chunkSize));
        }
        
        let tasks: client.KeywordsDataTaskRequestInfo[] = [];

        // Loop through the chunks
        for (const chunk of chunks) {
            let keys: string[] = chunk.map(row => row.keyword);
            let task = new client.KeywordsDataTaskRequestInfo(); 
            task.location_code = 2840;
            task.language_code = "en";
            task.keywords = keys;
            task.postback_data = "https://your.pingback.url";
            tasks.push(task);
            // 10 Tasks where every task will have 1000 keywords
            // 1000 x 10 = 10k keywords handed by one lambda function
        }

        let resp = await sendKeywordDataTasks(keywordDataApi, tasks);
        if(!resp){
            console.log("Failed to send tasks.");
            batchFailed = true;
        }else{
            resp?.tasks?.forEach(async (task, index) => {
                if(task?.status_code == 20000){
                    let keys = task?.data?.keywords;
                    // Loop through the keys and find the id from the keywordMap
                    let keywordIds: string[] = [];
                    keys.forEach(key => {
                        keywordIds.push(keywordMap[key]);
                    });
                    // Update the keyword ids in the clickhouse
                    await updateByKeywordIds(ch_client, keywordIds, task.id);
                }
            });
        }
    
        if(batchFailed){
            console.log("Batch failed");
            return {
                statusCode: 500,
                body: JSON.stringify('Batch failed'),
            };
        }else{
            return {
                statusCode: 200,
                body: JSON.stringify('Batch successful'),
            };
        }

    }catch(e){
        console.log(e);
        return {
            statusCode: 500,
            body: JSON.stringify('Batch failed'),
        };
    }
};

const sendKeywordDataTasks = async (keywordDataApi: client.KeywordsDataApi, tasks: client.SerpTaskRequestInfo[], rateLimitRetry = 0) => {
    try{
        let response = await keywordDataApi.googleAdsSearchVolumeTaskPost(tasks);
        if(response?.status_code == 20000){
            console.log("Tasks sent successfully");
            return response;
        }else if (response?.status_code == 40202){
            // sleep for 5 seconds and retry
            await new Promise(resolve => setTimeout(resolve, 5000));
            if(rateLimitRetry < 15){
                await sendKeywordDataTasks(keywordDataApi, tasks, rateLimitRetry + 1);
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