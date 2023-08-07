import controllerHandler from '@lib/controller/handler.js';
import * as exchangeService from '@service/exchange.js';

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
    exchangeService.findAll(query)
);

export const count = controllerHandler<FindAllRequestArgs>(({ query }) =>
    exchangeService.count(query)
);

export const countSecurity = controllerHandler<
    FindAllRequestArgs & FindOneRequestArgs
>(({ query, params }) => exchangeService.countSecurity(params.id, query));

export const filterChoices = controllerHandler<FilterChoicesRequestArgs>(
    ({ params }) => exchangeService.filterChoices(params.field)
);

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    exchangeService.findOne(params.id)
);

export const findSecurity = controllerHandler<FindOneRequestArgs>(
    ({ params }) => exchangeService.findSecurity(params.id)
);
