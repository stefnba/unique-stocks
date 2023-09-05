import controllerHandler from '@lib/controller/handler.js';
import * as securityService from '@service/security.js';

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
    securityService.findAll(query)
);

export const findAllListingForSecurity = controllerHandler<
    FindAllRequestArgs & FindOneRequestArgs
>(({ query, params }) =>
    securityService.findAllListingForSecurity(params.id, query)
);

export const filterChoices = controllerHandler<FilterChoicesRequestArgs>(
    ({ params }) => securityService.filterChoices(params.field)
);

export const count = controllerHandler<
    Omit<FindAllRequestArgs, 'page' | 'pageSize'>
>(({ query }) => securityService.count(query));

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    securityService.findOne(params.id)
);

export const findOneListing = controllerHandler<FindOneRequestArgs>(
    ({ params }) => securityService.findOneListing(params.id)
);
