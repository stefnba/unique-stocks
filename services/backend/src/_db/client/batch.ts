import QueryBuilder from './builder';
import {
    DatabaseClient,
    DatabaseOptions,
    BatchQueryCallback,
    BatchClient
} from './types';

export default class PostgresBatchQuery {
    private db: DatabaseClient | BatchClient;
    private options: DatabaseOptions;
    private isTransaction: boolean;

    constructor(db: DatabaseClient | BatchClient, options: DatabaseOptions) {
        this.db = db;
        this.options = options;
        this.isTransaction = false;
    }

    private initQuery(t: BatchClient) {
        return new QueryBuilder(t, this.options);
    }

    async executeTransaction<T = void>(
        callback: BatchQueryCallback
    ): Promise<T> {
        this.isTransaction = true;

        return this.db.task(async (t) => {
            const query = this.initQuery(t);
            // BEGIN
            await query
                .run('BEGIN TRANSACTION;')
                .none()
                .then(() => {
                    if (this.options.transaction?.onBegin) {
                        this.options.transaction?.onBegin();
                    }
                });

            return callback(query)
                .then(async (r) => {
                    await query.run('COMMIT;').none();

                    if (this.options.transaction?.onCommmit) {
                        this.options.transaction?.onCommmit();
                    }
                    return r;
                })
                .catch(async (err) => {
                    await query.run('ROLLBACK;').none();
                    if (this.options.transaction?.onRollback) {
                        this.options.transaction?.onRollback(err);
                    } else {
                        throw err;
                    }
                    return Promise.reject(err);
                });
        });
    }

    /**
     * Starts and executes batch query
     * @param callback function
     * Function that contains batch queries which are executed in single connection pool
     * @returns
     */
    async executeBatch<T = void>(callback: BatchQueryCallback): Promise<T> {
        if (!this.db) throw new Error('Client not defined');

        return this.db
            .task(async (t) => {
                const query = this.initQuery(t);
                return callback(query);
            })
            .then((result) => result)
            .catch((err) => {
                throw err;
            });
    }
}
