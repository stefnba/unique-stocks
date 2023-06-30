import { LogDbQuery } from '@db/query';

const buildOrFilter = (filter: object) =>
    Object.entries(filter).reduce((accumulator, [field, filterValue]) => {
        if (Array.isArray(filterValue)) {
            return {
                ...accumulator,
                [field]: { $in: filterValue }
            };
        }

        return {
            ...accumulator,
            [field]: filterValue
        };
    }, {});

export const findAll = async (filter: object) => {
    return LogDbQuery.findAll(buildOrFilter(filter));
};

export const findOne = async (id: string) => {
    return LogDbQuery.findOne(id);
};

export const getFieldChoices = async (field: string, filter: object) => {
    return {
        field,
        choices: await LogDbQuery.getFieldChoices(field, buildOrFilter(filter))
    };
};
