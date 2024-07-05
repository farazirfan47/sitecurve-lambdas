import { MongoClient } from "mongodb";
import { SerpJob } from "./types";

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

export const updateDomainStatuses = async (
  client: MongoClient,
  serpJobs: SerpJob[],
  status: string
): Promise<void> => {
  try {
    const db = client.db("content-parse");
    const collection = db.collection("serp_jobs");

    const bulkOps = serpJobs.map((serpJob) => ({
      updateOne: {
        filter: { serp_id: serpJob.serp_id },
        update: { $set: { status: status } },
      },
    }));

    await collection.bulkWrite(bulkOps);
  } catch (e) {
    console.error("MongoDB update failed");
    throw e;
  }
};
