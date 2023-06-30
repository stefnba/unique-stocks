import { dbLogs } from '@app/db/db.js';

import { ObjectId } from 'mongodb';

export default {
    findAll: (filter?: object) =>
        dbLogs.find(filter).limit(25).sort({ loggedAt: -1 }).toArray(),
    findOne: (id: string) => dbLogs.findOne({ _id: new ObjectId(id) }),
    getFieldChoices: (field: string, filter?: object) =>
        dbLogs.distinct(field, filter)
};
