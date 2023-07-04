import { dbLogs } from '@app/db/db.js';

import { ObjectId } from 'mongodb';

export default {
    findAll: (filter: object, page: number, pageSize: number) =>
        dbLogs
            .find(filter)
            .sort({ loggedAt: -1 })
            .skip((page - 1) * pageSize)
            .limit(pageSize)
            .toArray(),
    findOne: (id: string) => dbLogs.findOne({ _id: new ObjectId(id) }),
    getCount: (filter: object) => dbLogs.countDocuments(filter),
    getFieldChoices: (field: string, filter?: object) =>
        dbLogs.distinct(field, filter)
};
