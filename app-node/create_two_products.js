
const { MongoClient } = require('mongodb');

// Prefer RS-aware connection so writes follow the PRIMARY automatically
const uri = process.env.MONGO_URI 
  || 'mongodb://appuser:appuserpassword@127.0.0.1:27030,127.0.0.1:27031,127.0.0.1:27032/appdb?replicaSet=rs0';

async function run() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('appdb');
    const products = db.collection('products');

    const docs = [
      { name: 'Product_' + Math.random().toString(36).slice(2, 10), createdAt: new Date() },
      { name: 'Product_' + Math.random().toString(36).slice(2, 10), createdAt: new Date() }
    ];

    const result = await products.insertMany(docs, { ordered: true });
    console.log(`Inserted ${result.insertedCount || Object.keys(result.insertedIds).length} products:`);
    console.log(result.insertedIds);
  } catch (err) {
    console.error('Error:', err);
    process.exitCode = 1;
  } finally {
    await client.close();
  }
}

run();
