import { StocksDbQuery } from '@db/query';

const ExchangeQuery = StocksDbQuery.repos.security;

export const findAll = () => {
    return ExchangeQuery.findAll();
};

export const findOne = (exchangeId: number) => {
    return ExchangeQuery.findOne(exchangeId);
};
