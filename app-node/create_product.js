const { MongoClient } = require('mongodb');
const uri = 'mongodb://appuser:appuserpassword@127.0.0.1:27032/appdb?directConnection=true';

async function run() {
  const client = new MongoClient(uri, { useUnifiedTopology: true });
  try {
    await client.connect();
    const db = client.db('appdb');
    const products = db.collection('products');
    const randomName = 'Product_' + Math.random().toString(36).substring(2, 10);
    const result = await products.insertOne({ name: randomName, createdAt: new Date() });
    console.log('Inserted product:', result.insertedId, 'with name:', randomName);
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
  }
}

run();
