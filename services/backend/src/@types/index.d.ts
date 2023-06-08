import userAgentParser from 'ua-parser-js';
export {};

declare global {
    // namespace Express {
    //     interface Request {
    //         user?: string;
    //     }
    // }

    namespace NodeJS {
        interface ProcessEnv {
            NODE_ENV: 'development' | 'production';

            APP_PORT: string;

            // DB_HOST: string;
            // DB_PORT: string;
            // DB_ROOT_PASSWORD: string;
            // DB_NAME: string;
            // DB_SCHEMA: string;

            DB_ADMIN_USER: string;
            DB_ADMIN_PASSWORD: string;
            DB_APP_USER: string;
            DB_APP_PASSWORD: string;

            STOCKS_DB_HOST: string;
            STOCKS_DB_PORT: string;
            STOCKS_DB_NAME: string;
            STOCKS_DB_APP_USER: string;
            STOCKS_DB_APP_PASSWORD: string;
        }
    }
}

/**
 * Required for Morgan to also log user and ip
 */

import 'http';
declare module 'http' {
    interface IncomingMessage {
        user?: string;
        ip: string;
        userAgent: userAgentParser.IResult;
        correlationId: string;
    }
}

import 'express';

declare module 'express' {
    interface Request {
        user?: string;
    }
}
