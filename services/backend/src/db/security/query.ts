import { DatabaseRepository } from '@app/db/client/index.js';

import { fileDirName } from '@sharedUtils/module.js';

export default class ExchangeRepository extends DatabaseRepository {
    table = 'data.security';

    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        findAll: this.sqlFile('findAll.sql'),
        findListing: this.sqlFile('findListing.sql'),
        findOne: this.sqlFile('findOne.sql'),
        findOneListing: this.sqlFile('findOneListing.sql')
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

    async findAllListingForSecurity(
        id: number,
        filter?: object,
        page?: number,
        pageSize?: number
    ) {
        return this.query
            .find(this.queries.findListing, {
                filter: {
                    filter: {
                        ...filter,
                        id
                    },
                    filterSet: {
                        ...this.filterSet,
                        id: {
                            operator: 'EQUAL',
                            column: 'security_id',
                            alias: 'security_ticker'
                        }
                    }
                },
                pagination: {
                    page,
                    pageSize
                },
                search: {
                    table: ['data', 'security'],
                    joinColumns: 'id'
                },
                ordering: [{ column: 'quote_source', logic: 'ASC' }]
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

    async findOneListing(id: number) {
        return this.query
            .find(this.queries.findOneListing, {
                filter: {
                    filter: {
                        id
                    },
                    filterSet: {
                        id: {
                            column: 'id',
                            operator: 'EQUAL',
                            alias: 'security_listing'
                        }
                    }
                }
            })
            .oneOrNone();
    }
}
