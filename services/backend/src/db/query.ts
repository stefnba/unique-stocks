import { dbStocks } from '@app/db/db.js';

import ExchangeRepository from './exchange/query.js';
import EntityRepository from './entity/query.js';
import LogRepository from './log/query.js';

export const StocksDbQuery = dbStocks.addRepositories({
    exchange: ExchangeRepository,
    entity: EntityRepository
});

export const LogDbQuery = LogRepository;
