import { NextFunction, Response, Request } from 'express';
import queryString from 'query-string';

import { ControllerOptions } from './types';
import { constants } from '../../_app';

export interface ControllerRequestParams {
    body?: object;
    query?: object;
    params?: object;
}

/**
 * Controller shortcut used in routes. Through controllerFunc(), provides access to userId and other request info such as
 * body, params, query
 * @param req
 * @param res
 * @param next
 * @param controllerFunc
 * @param options
 */
const Controller = async (
    req: Request,
    res: Response,
    next: NextFunction,
    controllerFunc: (
        user: string,
        controllerRequestParams: ControllerRequestParams
    ) => Promise<unknown>,
    options: ControllerOptions = {
        statusCode: null,
        responseMsg: null,
        hideResponseMsg: false
    }
) => {
    const { body, params, url } = req;

    // Query is pared via url
    const parsedQuery = queryString.parse(url.split('?')[1], {
        arrayFormat: 'comma'
    });

    const controllerRequestParams = {
        body,
        query: parsedQuery,
        params
    };

    return controllerFunc('user', controllerRequestParams)
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

            return res
                .status(
                    options.statusCode ||
                        constants.httpMethodStatusMapping[method]
                )
                .json(response);
        })
        .catch((err) => next(err));
};

export default Controller;
