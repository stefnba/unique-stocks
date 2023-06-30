import { Response, Request, NextFunction } from 'express';
import queryString from 'query-string';

import { httpMethodStatusMapping } from '@app/constants/constants.js';

import {
    ControllerOptions,
    ControllerHandlerFunc,
    RequestArgsBase
} from './types.js';

const controllerHandler =
    <RequestArgs extends RequestArgsBase = RequestArgsBase>(
        controllerFunc: ControllerHandlerFunc<RequestArgs>,
        options: ControllerOptions = {
            statusCode: undefined,
            responseMsg: undefined,
            hideResponseMsg: false
        }
    ) =>
    async (req: Request, res: Response, next: NextFunction) => {
        const user = req.user;

        return controllerFunc(user, {
            body: req.body,
            params: req.params,
            query: queryString.parse(req.url.split('?')[1], {
                arrayFormat: 'comma'
            })
        })
            .then((r) => {
                const { method } = req;

                let response = r;

                if (!options.hideResponseMsg) {
                    if (method === 'POST') {
                        response = {
                            message: options.responseMsg || 'CREATED',
                            data: r
                        };
                    }
                    if (method === 'PUT') {
                        response = {
                            message: options.responseMsg || 'UPDATED',
                            data: r
                        };
                    }
                    if (method === 'DELETE') {
                        response = {
                            message: options.responseMsg || 'DELETED',
                            data: r
                        };
                    }
                }

                res.status(
                    options.statusCode || httpMethodStatusMapping[method]
                ).json(response);
            })
            .catch((err) => next(err));
    };

export default controllerHandler;
