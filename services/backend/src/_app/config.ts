import dotenv from 'dotenv';
dotenv.config();

export default {
    app: {
        port: parseInt(process.env.APP_PORT)
    },
    database: {
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT),
        database: process.env.DB_NAME,
        user: process.env.DB_ADMIN_USER,
        password: process.env.DB_APP_PASSWORD,
        max: 30,

        app: {
            host: process.env.DB_HOST,
            port: parseInt(process.env.DB_PORT),
            database: process.env.DB_NAME,
            user: process.env.DB_ADMIN_USER,
            password: process.env.DB_APP_PASSWORD,
            max: 30
        },
        stocks: {
            ost: process.env.STOCKS_DB_HOST,
            port: process.env.STOCKS_DB_PORT,
            database: process.env.STOCKS_DB_NAME,
            user: process.env.STOCKS_DB_USER,
            password: process.env.STOCKS_DB_PASSWORD,
            max: 30
        }
    },
    cors: {
        origin: ['http://localhost:3000', 'https://unique-stocks.com'],
        allowedHeaders: ['Content-Type', 'Authorization'],
        credentials: true,
        // preflightContinue: true,
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
    }
};
