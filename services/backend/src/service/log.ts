import { LogDbQuery } from '@db/query';

const buildOrFilter = (filter: object, field?: string) => {
    // remove current field to show all options
    if (field && field in filter) {
        delete filter[field];
    }

    delete filter['page'];
    delete filter['pageSize'];

    return Object.entries(filter).reduce(
        (accumulator, [field, filterValue]) => {
            // IN filter for array
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
        },
        {}
    );
};

export const findAll = async (filter: {
    page?: string;
    pageSize?: string;
    [key: string]: unknown;
}) => {
    console.log(filter);
    const { page = '1', pageSize = '25', ...filters } = filter;

    return LogDbQuery.findAll(
        buildOrFilter(filters),
        parseInt(page),
        parseInt(pageSize)
    );
};

export const findOne = async (id: string) => {
    return LogDbQuery.findOne(id);
};

export const getCount = async (filter: object) => {
    const count = await LogDbQuery.getCount(filter);
    return { count };
};

export const getFieldChoices = async (field: string, filter: object) => {
    return {
        field,
        choices: await LogDbQuery.getFieldChoices(
            field,
            buildOrFilter(filter, field)
        )
    };
};
