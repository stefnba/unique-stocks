import dotenv from 'dotenv';
import PostgresClient from './client/index.js';

import { MongoClient } from 'mongodb';

dotenv.config();

const {
    DB_HOST,
    DB_NAME,
    DB_APP_USER,
    DB_APP_PASSWORD,
    DB_PORT,
    STOCKS_DB_PORT,
    LOG_DB_HOST,
    LOG_DB_PORT,
    LOG_DB_PASSWORD,
    LOG_DB_USER
} = process.env;

const connectionString = `mongodb://${LOG_DB_USER}:${LOG_DB_PASSWORD}@${LOG_DB_HOST}:${LOG_DB_PORT}`;

export const client = new MongoClient(connectionString);
const db = client.db('uniquestocks');

console.log(await db.admin().ping());

const dbLogs = db.collection('log');

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
            log: true
        }
    }
);

const dbStocks = new PostgresClient(
    {
        host: DB_HOST,
        port: Number(STOCKS_DB_PORT),
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
