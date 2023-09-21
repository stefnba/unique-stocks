import controllerHandler from '@lib/controller/handler.js';
import * as logService from '@service/log.js';

type FindOneRequestArgs = {
    params: { id: string };
};

type FindAllRequestArgs = {
    query: { page?: string; pageSize?: string; [key: string]: unknown };
};

type GetDistinctFieldChoicesRequestArgs = {
    params: { field: string };
    query: object;
};

export const findAll = controllerHandler<FindAllRequestArgs>(({ query }) =>
    logService.findAll(query)
);

export const getCount = controllerHandler<FindAllRequestArgs>(({ query }) =>
    logService.getCount(query)
);

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    logService.findOne(params.id)
);

export const getFieldChoices =
    controllerHandler<GetDistinctFieldChoicesRequestArgs>(({ params, query }) =>
        logService.getFieldChoices(params.field, query)
    );
