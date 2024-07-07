import { MongoClient, ObjectId } from "mongodb";
import { ParsedContentItem } from "./types";

const uri = process.env.MONGODB_URI || "mongodb://localhost:27017";

export const initMongoClient = async (): Promise<MongoClient> => {
  try {
    const client = await connectWithRetry();
    if (client) {
      return client;
    } else {
      throw new Error("MongoDB connection failed");
    }
  } catch (e) {
    console.error("MongoDB connection failed");
    throw e;
  }
};

async function connectWithRetry() {
  let attempts = 0;
  const maxRetries = 5;

  while (attempts < maxRetries) {
    try {
      const client = new MongoClient(uri, {
        tls: true,
        tlsAllowInvalidCertificates: true,
      });
      await client.connect();
      console.log("Connected successfully to server");
      return client;
    } catch (err) {
      console.error(`Connection attempt ${attempts + 1} failed:`, err);
      attempts++;
      if (attempts >= maxRetries) {
        throw new Error("Exceeded maximum retry attempts");
      }
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
    }
  }
}

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
