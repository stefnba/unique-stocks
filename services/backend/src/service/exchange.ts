import { StocksDbQuery } from '@db/query';

const ExchangeQuery = StocksDbQuery.repos.exchange;

export const findAll = () => {
    return ExchangeQuery.findAll();
};

export const findOne = (exchangeId: number) => {
    console.log(exchangeId);
    return ExchangeQuery.findOne(exchangeId);
};
