import { MongoClient, ObjectId } from "mongodb";

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

export const saveInMongoDB = async (
  client: MongoClient,
  serps: any,
  keywordId: string
) => {
  try {
    const db = client.db("sitecurve");
    const collection = db.collection("keywords");
    // In the keywords collection we have serps array that could be empty or have some SERP rows, we will upsert them, we need to upsert all serps in the serps key against id = keywordId
    const result = await collection.updateOne(
      { _id: new ObjectId(keywordId) },
      { $set: { serps: serps, serp_api_status: "DONE" } },
      { upsert: true }
    );
    console.log(`Inserted ${result.upsertedCount} documents`);
    console.log(`Updated ${result.modifiedCount} documents`);
    // Fetch the updated Serps array with ids
    const updatedDocument = await collection.findOne(
      { _id: new ObjectId(keywordId) },
      { projection: { serps: 1 } }
    );
    return updatedDocument?.serps;
  } catch (e) {
    console.error("Failed to save SERP rows in MongoDB");
    throw e;
  }
};
