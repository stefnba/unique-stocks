import morgan from 'morgan';

// import { logger } from '../_utils';

// Override the stream method by telling Morgan to use our custom logger instead of the console.log.
const stream = {
    write: (message: string) => {
        const {
            user: userId,
            correlationId,
            responseTime,
            userAgent,
            method,
            status,
            url,
            ...details
        } = JSON.parse(message);
        // return logger.http.info('REQUEST', {
        //     userId,
        //     correlationId: correlationId,
        //     http: {
        //         url,
        //         status,
        //         method,
        //         responseTime,
        //         userAgent
        //     },
        //     ...details
        // });
    }
};

const morganMiddleware = morgan(
    (tokens, req, res) =>
        JSON.stringify({
            method: tokens.method(req, res),
            url: tokens.url(req, res),
            status: parseInt(tokens.status(req, res), 10),
            responseTime: parseFloat(tokens['response-time'](req, res)),
            // user: req.user,
            contentLength: parseInt(tokens.res(req, res, 'content-length'), 10)
            // userAgent: req.userAgent,
            // ip: req.connection.remoteAddress,
            // ip: req.ip,
            // correlationId: req.correlationId
        }),
    {
        stream,
        skip: (req) => req.method === 'OPTIONS'
    }
);

export default morganMiddleware;
