import { MongoClient, ObjectId } from "mongodb";
import { ParsedContentItem } from "./types";

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
  serpJobs: ParsedContentItem[]
): Promise<void> => {
  try {
    console.log("Updating MongoDB");

    const db = client.db("sitecurve");
    const collection = db.collection("keywords");

    console.log("serpJobs", serpJobs);

    const bulkOps = serpJobs.map((serpJob) => ({
      updateOne: {
        filter: { "serps._id": new ObjectId(serpJob.serp_id) },
        update: {
          $set: {
            "serps.$[elem].content_parse_status": serpJob.status,
            "serps.$[elem].page_meta": serpJob.page_meta,
          },
        },
        arrayFilters: [{ "elem._id": new ObjectId(serpJob.serp_id) }],
      },
    }));

    await collection.bulkWrite(bulkOps);
    console.log("MongoDB update successful");
  } catch (e) {
    console.error("MongoDB update failed");
    throw e;
  }
};
