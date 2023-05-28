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
        max: 30
    }
};
