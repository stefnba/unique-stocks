import { Request, Response, NextFunction } from 'express';
import userAgentParser from 'ua-parser-js';

export default function userAgent(
    req: Request,
    _: Response,
    next: NextFunction
) {
    // (req.userAgent = userAgentParser(req.headers['user-agent'])), next();
    next();
}
