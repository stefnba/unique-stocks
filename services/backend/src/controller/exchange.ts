import controllerHandler from '@lib/controller/handler.js';
import * as logService from '@service/exchange.js';

type FindOneRequestArgs = {
    params: { id: number };
};

export const findAll = controllerHandler(() => logService.findAll());

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    logService.findOne(params.id)
);
