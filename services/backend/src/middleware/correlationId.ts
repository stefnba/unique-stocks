import cryptoRandomString from 'crypto-random-string';
import { Request, Response, NextFunction } from 'express';

/**
 * Appends a correlationId to every request.
 */
export default function correlationId(
    req: Request,
    _: Response,
    next: NextFunction
) {
    req.correlationId = cryptoRandomString({ length: 64, type: 'url-safe' });
    next();
}
