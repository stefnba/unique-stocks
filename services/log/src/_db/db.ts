import dotenv from 'dotenv';
import { MongoClient } from 'mongodb';

dotenv.config();

// const { DB_HOST, DB_NAME, DB_APP_USER, DB_APP_PASSWORD, DB_PORT } = process.env;

const connectionString = 'mongodb://localhost:27017/mydb';

const client = new MongoClient(connectionString);

let conn;
try {
    conn = client.connect();
    const db = await conn.db('sample_training');
} catch (e) {
    console.error(e);
}

export default db;
