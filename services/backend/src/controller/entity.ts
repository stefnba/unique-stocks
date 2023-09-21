import controllerHandler from '@lib/controller/handler.js';
import * as entityService from '@service/entity.js';

type FindOneRequestArgs = {
    params: { id: number };
};

export type FindAllRequestArgs = {
    query: { page: number; pageSize: number; [key: string]: unknown };
};

type FilterChoicesRequestArgs = {
    params: { field: string };
};

export const findAll = controllerHandler<FindAllRequestArgs>(({ query }) =>
    entityService.findAll(query)
);

export const filterChoices = controllerHandler<FilterChoicesRequestArgs>(
    ({ params }) => entityService.filterChoices(params.field)
);

export const count = controllerHandler<
    Omit<FindAllRequestArgs, 'page' | 'pageSize'>
>(({ query }) => entityService.count(query));

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    entityService.findOne(params.id)
);

export const findSecurity = controllerHandler<FindOneRequestArgs>(
    ({ params }) => entityService.findSecurity(params.id)
);
