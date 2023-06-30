import controllerHandler from '@lib/controller/handler.js';
import * as entityService from '@service/entity.js';

type FindOneRequestArgs = {
    params: { id: number };
};

export const findAll = controllerHandler(() => entityService.findAll());

export const findOne = controllerHandler<FindOneRequestArgs>((_, { params }) =>
    entityService.findOne(params.id)
);
