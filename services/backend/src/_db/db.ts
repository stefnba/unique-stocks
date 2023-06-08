import dotenv from 'dotenv';
import PostgresClient from './client/index.js';
import ExchangeRepository from '../exchange/db/query.js';
import EntityRepository from '../entity/db/query.js';

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

const _dbStocks = new PostgresClient(
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

const dbStocks = _dbStocks.addRepositories({
    exchange: ExchangeRepository,
    entity: EntityRepository
});

export { dbApp, dbStocks };
