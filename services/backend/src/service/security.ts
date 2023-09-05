import { StocksDbQuery } from '@db/query';
import type { FindAllRequestArgs } from '@controller/security.js';

const query = StocksDbQuery.repos.security;

export const findAll = (queryObject: FindAllRequestArgs['query']) => {
    const { page, pageSize, ...filter } = queryObject;

    return query.findAll(filter, page, pageSize);
};

export const findAllListingForSecurity = (
    id: number,
    queryObject: FindAllRequestArgs['query']
) => {
    const { page, pageSize, ...filter } = queryObject;

    return query.findAllListingForSecurity(id, filter, page, pageSize);
};

export const count = (queryObject: FindAllRequestArgs['query']) => {
    const { page, pageSize, ...filter } = queryObject;
    return query.count(filter);
};

export const filterChoices = (field: string) => {
    return query.filterChoices(field);
};

export const findOne = (id: number) => {
    return query.findOne(id);
};

export const findOneListing = (id: number) => {
    return query.findOneListing(id);
};
