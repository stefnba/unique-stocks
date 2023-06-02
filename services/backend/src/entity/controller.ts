import { Request, Response, NextFunction } from 'express';

import { Controller } from '../_lib';
import EntityService from './service';

type Body = {
    body: number;
};

const EntityServiceInstance = new EntityService();

const controller = {
    findAll: async (req: Request, res: Response, next: NextFunction) => {
        Controller(
            req,
            res,
            next,
            // (userId, { body }: Body) => ExchangeServiceInstance.findAll(userId),
            () => EntityServiceInstance.findAll()
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
            ) => EntityServiceInstance.findOne(params.id)
        );
    }
};

export default controller;
