import dotenv from 'dotenv';
dotenv.config();

export default {
    app: {
        port: parseInt(process.env.APP_PORT || '8000')
    },
    mongoDb: {
        host: process.env.MONGO_HOST || 'localhost',
        port: parseInt(process.env.MONGO_PORT),
        username: process.env.MONGO_USER,
        password: process.env.MONGO_PASSWORD,
        db: process.env.MONGO_DB
    }
};
