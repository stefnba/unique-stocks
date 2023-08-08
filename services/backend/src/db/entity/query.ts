import { DatabaseRepository } from '@app/db/client/index.js';
import { fileDirName } from '@sharedUtils/module.js';

export default class EntityRepository extends DatabaseRepository {
    // specify table name for this repository
    table = 'data.entity';
    // specify dir for SQL files
    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        find: this.sqlFile('find.sql'),
        findSecurity: this.sqlFile('findSecurity.sql')
    };

    filterSet = this.filterSet({
        headquarter_address_country: {
            column: 'headquarter_address_country',
            operator: 'INCLUDES'
        },
        search: {
            column: 'search_token',
            operator: 'FULL_TEXT_SEARCH'
        }
    });

    async findAll(filter?: object, page?: number, pageSize?: number) {
        return this.query
            .find(this.queries.find, {
                ordering: [{ column: 'name', logic: 'ASC' }],
                pagination: {
                    page,
                    pageSize
                },
                filter: {
                    filter,
                    filterSet: this.filterSet
                },
                search: {
                    table: ['data', 'entity'],
                    joinColumns: 'id'
                }
            })
            .many();
    }

    async findSecurity(entityId: number) {
        return this.query
            .find(this.queries.findSecurity, {
                params: {
                    entityId
                }
            })
            .many();
    }

    async findOne(entityId: number) {
        return this.query
            .find(this.queries.find, {
                filter: {
                    filter: {
                        id: entityId
                    },
                    filterSet: { id: 'EQUAL' }
                }
            })
            .oneOrNone();
    }

    async countSecurity(filter?: object) {
        return this.query.count(['data', 'entity_isin'], {
            filter,
            filterSet: {
                ...this.filterSet,
                entityId: {
                    column: 'entity_id',
                    operator: 'EQUAL'
                }
            }
        });
    }

    async count(filter?: object) {
        return this.query.count(['data', 'entity'], {
            filter,
            filterSet: this.filterSet
        });
    }

    async filterChoices(field: string, filter?: object) {
        const available_fields = {
            headquarter_address_country: 'default'
        };

        if (!Object.keys(available_fields).includes(field)) return [];

        return this.query.valueCounts({
            table: ['data', 'entity'],
            column: field,
            type: available_fields[field],
            filter: {
                filter,
                filterSet: this.filterSet
            }
        });
    }
}
