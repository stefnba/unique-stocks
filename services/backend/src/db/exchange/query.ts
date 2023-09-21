import { DatabaseRepository } from '@app/db/client/index.js';

import { fileDirName } from '@sharedUtils/module.js';

export default class ExchangeRepository extends DatabaseRepository {
    table = 'data.exchange';

    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        findAll: this.sqlFile('findAll.sql'),
        findOne: this.sqlFile('findOne.sql'),
        findSecurity: this.sqlFile('findSecurity.sql')
    };

    filterSet = this.filterSet({
        source: {
            column: 'source',
            operator: 'INCLUDES'
        },
        exchange_type: {
            column: 'operating_exchange_id',
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
                ordering: [{ column: 'name', logic: 'ASC' }],
                search: {
                    table: ['data', 'exchange'],
                    joinColumns: 'id'
                }
            })
            .many();
    }

    async findSecurity(exchangeId: number) {
        return this.query
            .find(this.queries.findSecurity, {
                filter: {
                    filter: {
                        exchangeId
                    },
                    filterSet: {
                        exchangeId: {
                            column: 'exchange_id',
                            operator: 'EQUAL',
                            alias: 'sl'
                        }
                    }
                }
            })
            .many();
    }

    async count(filter?: object) {
        return this.query.count(['data', 'exchange'], {
            filter,
            filterSet: this.filterSet
        });
    }

    async countSecurity(exchangeId: number, filter?: object) {
        return this.query.count(['data', 'security_listing'], {
            filter: {
                ...filter,
                exchangeId
            },
            filterSet: {
                ...this.filterSet,
                exchangeId: {
                    column: 'exchange_id',
                    operator: 'EQUAL'
                }
            }
        });
    }

    async filterChoices(field: string, filter?: object) {
        const available_fields = {
            source: 'default',
            is_virtual: 'default',
            operating_exchange_id: 'null'
        };

        if (!Object.keys(available_fields).includes(field)) return [];

        return this.query.valueCounts({
            table: ['data', 'exchange'],
            column: field,
            type: available_fields[field],
            filter: {
                filter,
                filterSet: this.filterSet
            }
        });
    }

    async findOne(exchangeId: number) {
        return this.query
            .find(this.queries.findOne, {
                filter: {
                    filter: {
                        id: exchangeId
                    },
                    filterSet: {
                        id: {
                            column: 'id',
                            operator: 'EQUAL',
                            alias: 'exchange'
                        }
                    }
                }
            })
            .oneOrNone();
    }
}
