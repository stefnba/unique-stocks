import MongoCollection from './client.js';

export default {
    user: new MongoCollection('user'),
    log: new MongoCollection('log')
};
