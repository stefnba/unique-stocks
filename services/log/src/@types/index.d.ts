export {};

declare global {
    namespace NodeJS {
        interface ProcessEnv {
            NODE_ENV: 'development' | 'production';

            APP_PORT: string;

            MONGO_HOST: string;
            MONGO_PORT: string;
            MONGO_USER: string;
            MONGO_PASSWORD: string;
            MONGO_DB: string;
        }
    }
}
