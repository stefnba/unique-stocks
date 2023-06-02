export {};

declare global {
    namespace NodeJS {
        interface ProcessEnv {
            NODE_ENV: 'development' | 'production';

            APP_PORT: string;

            DB_HOST: string;
            DB_PORT: string;
            DB_ROOT_PASSWORD: string;
            DB_NAME: string;
            DB_SCHEMA: string;

            DB_ADMIN_USER: string;
            DB_ADMIN_PASSWORD: string;
            DB_APP_USER: string;
            DB_APP_PASSWORD: string;
        }
    }
}
