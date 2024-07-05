import { MongoClient } from "mongodb";

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

export const saveInMongoDB = async (client: MongoClient, serps: any, keywordId: string) => {
  try {
    const db = client.db("sitecurve");
    const collection = db.collection("keywords");
    // In the keywords collection we have serps array that could be empty or have some SERP rows, we will upsert them, we need to upsert all serps in the serps key against id = keywordId
    const result = await collection.updateOne(
      { id: keywordId },
      { $set: { serps: serps } },
      { upsert: true }
    );
    console.log(`Inserted ${result.upsertedCount} documents`);
    console.log(`Updated ${result.modifiedCount} documents`);
    // Fetch the updated Serps array with ids
    const updatedDocument = await collection.findOne({ id: keywordId }, { projection: { serps: 1 } });
    return updatedDocument;
  } catch (e) {
    console.error("Failed to save SERP rows in MongoDB");
    throw e;
  }
};
