import { DatabaseRepository } from '../../_db/client';

export default class ExchangeRepository extends DatabaseRepository {
    // specify table name for this repository
    table = 'exchange';
    // specify dir for SQL files
    sqlFilesDir = [__dirname, 'sql'];

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
            .many();
    }
}
