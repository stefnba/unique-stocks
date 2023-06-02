// /* eslint-disable @typescript-eslint/no-unused-vars */
// import { Request, Response, NextFunction } from 'express';
// import { logger, Errors } from '../_utils';

// import Base from '../_utils/errors/base';
// import type { ClientErrorResponse } from '../_utils/errors/types';

// export default function errorHandlingMiddleware(
//     err: Base,
//     req: Request,
//     res: Response,
//     _next: NextFunction
// ) {
//     let response: ClientErrorResponse;

//     try {
//         if (err.isOperational) {
//             response = {
//                 error: err?.publicMessage || 'ERROR',
//                 errorId: err?.id || null,
//                 correlationId: req?.correlationId
//             };
//         } else {
//             response = {
//                 error: err?.publicMessage || 'ERROR'
//             };
//         }

//         if (err.publicData) {
//             response = {
//                 ...response,
//                 data: { ...err.publicData }
//             };
//         }

//         // Mimics error to throw ResponseError with correct rank
//         throw new Errors.Response({
//             userId: req?.user,
//             correlationId: req?.correlationId,
//             response,
//             cause: err
//         });
//     } catch (e) {
//         res.status(err?.httpStatus || 500).json(response);
//     }
// }
