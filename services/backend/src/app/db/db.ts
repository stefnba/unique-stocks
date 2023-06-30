import dotenv from 'dotenv';
import PostgresClient from './client/index.js';

import { MongoClient } from 'mongodb';

const connectionString = 'mongodb://root:password@localhost:27017';

export const client = new MongoClient(connectionString);
const db = client.db('uniquestocks');

const dbLogs = db.collection('log');

dotenv.config();

const { DB_HOST, DB_NAME, DB_APP_USER, DB_APP_PASSWORD, DB_PORT } = process.env;

const dbApp = new PostgresClient(
    {
        host: DB_HOST,
        port: Number(DB_PORT),
        database: String(DB_NAME),
        user: String(DB_APP_USER),
        password: String(DB_APP_PASSWORD)
    },
    {
        connect: {
            onFailed: (connection) => {
                console.error(
                    `Database Connection Error: ${connection.error?.message} (${connection.error?.type})`
                );
                console.error(connection.connection);
            },
            log: false
        }
    }
);

const dbStocks = new PostgresClient(
    {
        host: DB_HOST,
        port: 5871,
        database: 'stocks',
        user: 'admin',
        password: 'password'
    },
    {
        connect: {
            onFailed: (connection) => {
                console.error(
                    `Database Connection Error: ${connection.error?.message} (${connection.error?.type})`
                );
                console.error(connection.connection);
            },
            log: true
        }
    }
);

export { dbApp, dbStocks, dbLogs };
