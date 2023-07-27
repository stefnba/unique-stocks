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

    async findAll() {
        return this.query
            .find(this.queries.find, {
                pagination: {
                    pageSize: 25
                },
                ordering: [{ column: 'name', logic: 'ASC' }],
                filter: {
                    filter: {
                        // legal_address_country: 'DE'
                    },
                    filterSet: { legal_address_country: 'EQUAL' }
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

    async findOne(exchangeId: number) {
        return this.query
            .find(this.queries.find, {
                filter: {
                    filter: {
                        id: exchangeId
                    },
                    filterSet: { id: 'EQUAL' }
                }
            })
            .oneOrNone();
    }
}
