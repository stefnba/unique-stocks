import { ExchangeQuery } from './data.js';

export default class ExchangeService {
    async findAll() {
        return ExchangeQuery.findAll();
    }
    async findOne(exchangeId: number) {
        console.log(exchangeId);
        return ExchangeQuery.findOne(exchangeId);
    }
}
