import { DatabaseRepository } from '@app/db/client/index.js';
import { fileDirName } from '@sharedUtils/module.js';

export default class EntityRepository extends DatabaseRepository {
    // specify table name for this repository
    table = 'data.entity';
    // specify dir for SQL files
    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        find: this.sqlFile('find.sql')
    };

    async findAll() {
        return this.query
            .find(this.queries.find, {
                pagination: {
                    pageSize: 3
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
