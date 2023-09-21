import { StocksDbQuery } from '@db/query';
import type { FindAllRequestArgs } from '@controller/exchange.js';

const query = StocksDbQuery.repos.exchange;

export const findAll = (queryObject: FindAllRequestArgs['query']) => {
    const { page, pageSize, ...filter } = queryObject;
    return query.findAll(filter, page, pageSize);
};

export const count = (queryObject: FindAllRequestArgs['query']) => {
    const { page, pageSize, ...filter } = queryObject;
    return query.count(filter);
};

export const countSecurity = (
    exchangeId: number,
    queryObject: FindAllRequestArgs['query']
) => {
    const { page, pageSize, ...filter } = queryObject;
    return query.countSecurity(exchangeId, filter);
};

export const filterChoices = (field: string) => {
    return query.filterChoices(field);
};

export const findOne = (exchangeId: number) => {
    return query.findOne(exchangeId);
};

export const findSecurity = (id: number) => {
    return query.findSecurity(id);
};
