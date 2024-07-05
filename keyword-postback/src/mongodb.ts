import { MongoClient } from "mongodb";
import { KeywordResult } from "./types";

const uri = process.env.MONGODB_URI || "mongodb://localhost:27017";

export const initMongoClient = async (): Promise<MongoClient> => {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    console.log("MongoDB connected");
    return client;
  } catch (e) {
    console.error("MongoDB connection failed");
    throw e;
  }
};

export const saveInMongoDB = async (
  client: MongoClient,
  keywordResults: KeywordResult[],
  taskId: string
) => {
  // Update sitecurve database and collection keywords with the keywordResults where task_id is taskId and keyword is keyword_result.keyword
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  const bulkOps = keywordResults.map((keywordResult) => ({
    updateOne: {
      filter: {
        task_id: taskId,
        keyword: keywordResult.keyword,
      },
      update: {
        $set: keywordResult,
      },
      upsert: true
    },
  }));
  const result = await collection.bulkWrite(bulkOps);
  console.log(`Inserted ${result.insertedCount} documents`);
  console.log(`Updated ${result.modifiedCount} documents`);
  console.log(`Upserted ${result.upsertedCount} documents`);
  return result;
};
