import { EntityQuery } from './data';

export default class EntityService {
    async findAll() {
        return EntityQuery.findAll();
    }
    async findOne(exchangeId: number) {
        console.log(exchangeId);
        return EntityQuery.findOne(exchangeId);
    }
}
