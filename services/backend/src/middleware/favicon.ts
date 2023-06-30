import { Request, Response, NextFunction } from 'express';

/**
 * Prevents favicon error.
 */
export default function ignoreFaviconMiddleware(
    req: Request,
    res: Response,
    next: NextFunction
) {
    if (req.url === '/favicon.ico') {
        res.status(204).end();
        return;
    }
    next();
}
