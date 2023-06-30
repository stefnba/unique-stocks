import { Request, Response } from 'express';

import * as logService from '../service/log.js';

export const findAll = async (req: Request, res: Response) => {
    const result = await logService.findAll();
    res.json(result);
};

export const findOne = async (req: Request, res: Response) => {
    res.send('NOT IMPLEMENTED');
};

export const addOne = async (req: Request, res: Response) => {
    const response = await logService.addOne(req.body);
    res.json(response);
};

export const getDistinctValues = async (req: Request, res: Response) => {
    const response = await logService.getDistinctValues(req.body);
    res.json(response);
};
