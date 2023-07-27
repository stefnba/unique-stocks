import { DatabaseRepository } from '@app/db/client/index.js';

import { fileDirName } from '@sharedUtils/module.js';

export default class ExchangeRepository extends DatabaseRepository {
    table = 'data.exchange';

    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        findAll: this.sqlFile('findAll.sql'),
        findOne: this.sqlFile('findOne.sql')
    };

    async findAll() {
        return this.query
            .find(this.queries.findAll, {
                // pagination: {
                //     pageSize:
                // }
            })
            .many();
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
