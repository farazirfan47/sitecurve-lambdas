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

export const getKeywords = async (
  client: MongoClient,
  keyword_ids: number[]
): Promise<any> => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  return await collection
    .find({ id: { $in: keyword_ids } }, { projection: { id: 1, keyword: 1 } })
    .toArray();
};

export const updateKeywordsStatus = async (
  client: MongoClient,
  chunk: any[],
  status: string
): Promise<void> => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  await Promise.all(
    chunk.map(async (row) => {
      await collection.updateOne({ id: row.id }, { $set: { status: status } });
    })
  );
};
