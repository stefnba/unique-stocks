import {
    MongoClient,
    Collection,
    Filter,
    Document,
    ObjectId,
    WithId,
    Db
} from 'mongodb';

const connectionString = 'mongodb://root:example@localhost:27017';

export const client = new MongoClient(connectionString);
const db = client.db('uniquestocks');

export default class MongoCollection {
    private collection: Collection;

    constructor(collection: string) {
        this.collection = db.collection(collection);
    }

    async findOne(filter: Filter<Document> = {}) {
        return this.collection.findOne(filter);
    }

    async addOne(data: object) {
        return this.collection.insertOne(data);
    }

    async findMany(filter: Filter<Document> = {}) {
        const result = this.collection.find(filter);
        return result.toArray();
    }
}
