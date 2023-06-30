import {
    MongoClient,
    Collection,
    Filter,
    Document,
    ObjectId,
    WithId,
    Db
} from 'mongodb';

import config from '@config';

const buildConnectionString = () => {
    const { host, password, port, username } = config.mongoDb;
    return `mongodb://${username}:${password}@${host}:${port}`;
};

export const client = new MongoClient(buildConnectionString());
export const db = client.db(config.mongoDb.db);

export default class MongoCollection {
    collection: Collection;

    constructor(collection: string) {
        this.collection = db.collection(collection);
    }

    async distinct(field: string) {
        return this.collection.distinct(field);
    }

    async findOne(filter: Filter<Document> = {}) {
        return this.collection.findOne(filter);
    }

    async addOne(data: object) {
        return this.collection.insertOne(data);
    }

    async findMany(filter: Filter<Document> = {}, limit = 25, page = 1) {
        const result = this.collection.find(filter).sort({ loggedAt: -1 });

        if (page > 1) {
            result.skip((page - 1) * limit);
        }

        if (limit) {
            result.limit(limit);
        }

        return result.toArray();
    }
}
