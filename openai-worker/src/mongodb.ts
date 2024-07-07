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

export const fetchSerpsFromDb = async (
  client: MongoClient,
  serpIds: string[]
) => {
  const db = client.db("sitecurve");
  const collection = db.collection("keywords");
  // I have a following collection structure:
  // {_id: ObjectId("5f9b3b3b3b3b3b3b3b3b3b3b"), id: "123", keyword: "keyword1", serps: [{id: "1", position: 1}, {id: "2", position: 2}]}
  // I want to find all documents where serps.id is in serpIds array and return on matching serp objects and not the entire document
  // Convert serpIds to ObjectId instances
  const serpObjectIds = serpIds.map((id) => new ObjectId(id));
  const serps = await collection
    .aggregate([
      { $match: { "serps._id": { $in: serpObjectIds } } },
      {
        $project: {
          _id: 1,
          serps: {
            $filter: {
              input: "$serps",
              as: "serp",
              cond: { $in: ["$$serp._id", serpObjectIds] },
            },
          },
        },
      },
    ])
    .toArray();
  // Merge all serps into a single array
  return serps.reduce((acc, curr) => acc.concat(curr.serps), []);
};
