import { StocksDbQuery } from '@db/query';

const EntityQuery = StocksDbQuery.repos.entity;

export const findAll = () => {
    return EntityQuery.findAll();
};

export const findOne = (id: number) => {
    return EntityQuery.findOne(id);
};

export const findSecurity = (id: number) => {
    return EntityQuery.findSecurity(id);
};
