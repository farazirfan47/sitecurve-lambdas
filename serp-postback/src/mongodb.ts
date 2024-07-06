import { MongoClient, ObjectId } from "mongodb";

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
      { _id: new ObjectId(keywordId) },
      { $set: { serps: serps, serp_api_status: "DONE"} },
      { upsert: true }
    );
    console.log(`Inserted ${result.upsertedCount} documents`);
    console.log(`Updated ${result.modifiedCount} documents`);
    // Fetch the updated Serps array with ids
    const updatedDocument = await collection.findOne({ _id: new ObjectId(keywordId) }, { projection: { serps: 1 } });
    return updatedDocument?.serps;
  } catch (e) {
    console.error("Failed to save SERP rows in MongoDB");
    throw e;
  }
};
