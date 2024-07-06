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
    const db = client.db("sitecurve");
    const collection = db.collection("keywords");


    // Here is my collection structure
    // {_id: ObjectId("5f9), search_volume: 100, serps: [ { _id: 1, content_parse_status: "PENDING" }] }
    // I have to update the content_parse_status of the serp object in the serps array

    const bulkOps = serpJobs.map((serpJob) => ({
      updateOne: {
        filter: {
          "serps._id": serpJob.serp_id,
        },
        update: {
          $set: {
            "serps.$[elem].content_parse_status": status,
          },
        },
        arrayFilters: [
          {
            "elem._id": serpJob.serp_id,
          },
        ]
      },
    }));

    await collection.bulkWrite(bulkOps);
    console.log("SerpJob statuses updated in MongoDB");

  } catch (e) {
    console.error("MongoDB update failed");
    throw e;
  }
};
