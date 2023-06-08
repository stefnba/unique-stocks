import { Request, Response, NextFunction } from 'express';

import { Controller } from '../_lib/index.js';
import ExchangeService from './service.js';

type Body = {
    body: number;
};

const ExchangeServiceInstance = new ExchangeService();

const controller = {
    findAll: async (req: Request, res: Response, next: NextFunction) => {
        Controller(
            req,
            res,
            next,
            // (userId, { body }: Body) => ExchangeServiceInstance.findAll(userId),
            () => ExchangeServiceInstance.findAll()
        );
    },
    findOne: async (req: Request, res: Response, next: NextFunction) => {
        Controller(
            req,
            res,
            next,
            // (userId, { body }: Body) => ExchangeServiceInstance.findAll(userId),
            (
                _,
                {
                    params
                }: {
                    params: {
                        id: number;
                    };
                }
            ) => ExchangeServiceInstance.findOne(params.id)
        );
    }
};

export default controller;
