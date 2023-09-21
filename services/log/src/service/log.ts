import collections from '@root/_db/collections.js';
import {
    capitalizeKeyFields,
    populateRequiredFields
} from 'utils/log/index.js';

export const findAll = async () => {
    return collections.log.findMany();
};

/**
 * Add a log record to db.
 * @param data log record
 * @returns
 */
export const addOne = async (data: Record<string, any>) => {
    data['created'] = new Date(data['created'] * 1000);

    populateRequiredFields(['event', 'message'], data);
    capitalizeKeyFields(['service', 'level'], data);

    console.log(data);

    const logRecord = await collections.log.addOne(data);
    return logRecord;
};

export const getDistinctValues = async (params: any) => {
    const values = await collections.log.distinct('level');
    return values;
};
