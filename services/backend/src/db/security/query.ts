import { DatabaseRepository } from '@app/db/client/index.js';

import { fileDirName } from '@sharedUtils/module.js';

export default class ExchangeRepository extends DatabaseRepository {
    table = 'data.security';

    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        findAll: this.sqlFile('findAll.sql'),
        findOne: this.sqlFile('findOne.sql')
    };

    filterSet = this.filterSet({
        security_type_id: {
            column: 'security_type_id',
            operator: 'INCLUDES'
        },
        isin: {
            column: 'isin',
            operator: 'INCLUDES_NULL'
        },
        search: {
            column: 'search_token',
            operator: 'FULL_TEXT_SEARCH'
        }
    });

    async findAll(filter?: object, page?: number, pageSize?: number) {
        return this.query
            .find(this.queries.findAll, {
                filter: {
                    filter,
                    filterSet: this.filterSet
                },
                pagination: {
                    page,
                    pageSize
                },
                search: {
                    table: ['data', 'security'],
                    joinColumns: 'id'
                }
            })
            .many();
    }

    async filterChoices(field: string, filter?: object) {
        const available_fields = {
            security_type_id: 'default',
            isin: 'null'
        };

        if (!Object.keys(available_fields).includes(field)) return [];

        return this.query.valueCounts({
            table: ['data', 'security'],
            column: field,
            type: available_fields[field],
            filter: {
                filter,
                filterSet: this.filterSet
            }
        });
    }

    async count(filter?: object) {
        return this.query.count(['data', 'security'], {
            filter,
            filterSet: this.filterSet
        });
    }

    async findOne(id: number) {
        return this.query
            .find(this.queries.findOne, {
                filter: {
                    filter: {
                        id
                    },
                    filterSet: {
                        id: {
                            column: 'id',
                            operator: 'EQUAL',
                            alias: 'security'
                        }
                    }
                }
            })
            .oneOrNone();
    }
}
