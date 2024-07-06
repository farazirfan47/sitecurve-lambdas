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

export const getKeywords = async (
  client: MongoClient,
  keyword_ids: number[]
): Promise<any> => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  const objectIdArray = keyword_ids.map((id) => new ObjectId(id));
  let result = await collection
    .find(
      { _id: { $in: objectIdArray } },
      { projection: { _id: 1, keyword: 1 } }
    )
    .toArray();
    // Convert _id to string
  return result.map((row) => {
    return {
      _id: row._id.toString(),
      keyword: row.keyword,
    };
  });
};

export const updateKeywordsStatus = async (
  client: MongoClient,
  keywordIds: any[],
  status: string
): Promise<void> => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  const bulkOperations = keywordIds.map((row) => ({
    updateOne: {
      filter: { _id: new ObjectId(row._id) },
      update: { $set: { status: status } },
    },
  }));
  await collection.bulkWrite(bulkOperations);
  console.log("Keywords status updated in MongoDB");
};

export const addTaskIdToKeywords = async (
  client: MongoClient,
  keywordIds: any[],
  taskId: string
): Promise<void> => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  // Update in bulk
  const bulkOperations = keywordIds.map((keywordId) => ({
    updateOne: {
      filter: { _id: new ObjectId(keywordId) },
      update: { $set: { dfsTaskId: taskId } },
    },
  }));
  await collection.bulkWrite(bulkOperations);
  console.log("Task ID added to keywords in MongoDB");
};
