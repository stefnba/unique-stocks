import { DatabaseRepository } from '@app/db/client/index.js';

import { fileDirName } from '@sharedUtils/module.js';

export default class ExchangeRepository extends DatabaseRepository {
    table = 'data.exchange';

    sqlFilesDir = [fileDirName(import.meta).__dirname, 'sql'];

    queries = {
        find: this.sqlFile('find.sql')
    };

    async findAll() {
        return this.query
            .find(this.queries.find, {
                // pagination: {
                //     pageSize:
                // }
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
