import PostgresBatchQuery from './batch.js';
import {
    DatabaseClient,
    DatabaseOptions,
    FindQueryParams,
    AddQueryParams,
    UpdateQueryParams,
    DataInput,
    BatchQueryCallback,
    BatchClient,
    QueryInput,
    FilterInput,
    CountQueryFilter
} from './types.js';
import Query from './query.js';

import {
    concatenateQuery,
    pgFormat,
    buildUpdateInsertQuery,
    tableName
} from './utils.js';
import pagination from './pagination.js';
import ordering from './ordering.js';
import { buildFilters } from './filter.js';

import FullTextSearch from './fullTextSearch.js';

/**
 * Simplifies construction of pg queries.
 */
export default class QueryBuilder<Model = undefined> {
    private client: DatabaseClient | BatchClient;
    private table?: string;
    private options: DatabaseOptions;
    private isBatch: boolean;

    constructor(
        client: DatabaseClient | BatchClient,
        options: DatabaseOptions,
        table?: string
    ) {
        this.table = table;

        this.isBatch = client.constructor.name === 'Task' ? true : false;
        this.client = client;
        this.options = options;
    }

    /**
     * Builds SELECT query that can be extended with filter and pagination
     * @param query string
     * @param params object
     * @returns
     *
     */
    find<M = Model extends undefined ? unknown : Model>(
        query: QueryInput,
        params?: FindQueryParams<Model extends undefined ? M : Model>
    ) {
        let _query = concatenateQuery([
            pgFormat(query, params?.params),
            { type: 'WHERE', query: buildFilters(params?.filter) }
        ]);

        // check and apply FullTextSearch
        const fts = new FullTextSearch(params?.search, params?.filter);

        if (fts.hasFullTextSearchQuery()) {
            // build query and append the initial LIMIT and OFFSET
            _query = concatenateQuery([
                fts.buildQuery(_query),
                {
                    query: pagination.pageSize(params?.pagination),
                    type: 'LIMIT'
                },
                { query: pagination.page(params?.pagination), type: 'OFFSET' }
            ]);
        } else {
            _query = concatenateQuery([
                _query,
                { type: 'ORDER', query: ordering(params?.ordering) },
                {
                    query: pagination.pageSize(params?.pagination),
                    type: 'LIMIT'
                },
                { query: pagination.page(params?.pagination), type: 'OFFSET' }
            ]);
        }

        console.log(_query);

        return new Query(this.client.any, this.isBatch, _query, {
            command: 'SELECT',
            table: this.table,
            log: this.options.query
        });
    }

    /**
     * Executes any query
     * @param query
     * @param params
     * @returns
     */
    run(query: QueryInput, params?: object) {
        return new Query(
            this.client.any,
            this.isBatch,
            pgFormat(query, params),
            {
                command: 'RUN',
                log: this.options.query,
                table: this.table
            }
        );
    }

    /**
     * Count the unique values in a table.
     * @param table name of table.
     * @param filter applied filters and filterSet to filter count.
     * @returns
     */
    valueCounts<M = Model extends undefined ? unknown : Model>(config: {
        table: string | [string, string];
        column: string;
        type?: 'default' | 'null';
        filter?: CountQueryFilter<Model extends undefined ? M : Model>;
        execute?: true;
    }): Promise<{ key: string; label?: string; count: number }[]>;
    valueCounts<M = Model extends undefined ? unknown : Model>(config: {
        table: string | [string, string];
        column: string;
        type?: 'default' | 'null';
        filter?: CountQueryFilter<Model extends undefined ? M : Model>;
        execute?: false;
    }): string;
    valueCounts<M = Model extends undefined ? unknown : Model>(config: {
        table: string | [string, string];
        column: string;
        type?: 'default' | 'null';
        filter?: CountQueryFilter<Model extends undefined ? M : Model>;
        execute?: boolean;
    }) {
        const type = config?.type || 'default';
        const execute = config?.execute !== undefined ? config?.execute : true;

        const _query = concatenateQuery([
            pgFormat(
                type === 'default'
                    ? 'SELECT $<column:name> AS key, COUNT(*)::INTEGER AS count FROM $<table>'
                    : "SELECT CASE WHEN $<column:name> IS NULL then 'NULL' ELSE 'NOT_NULL' END key, COUNT(*)::INTEGER AS count FROM $<table>",
                {
                    table: tableName(config.table),
                    column: config.column
                }
            ),
            { type: 'WHERE', query: buildFilters(config.filter) },
            pgFormat('GROUP BY 1 ORDER BY count DESC')
        ]);

        if (execute) {
            return new Query(this.client.any, this.isBatch, pgFormat(_query), {
                command: 'RUN',
                log: this.options.query,
                table: this.table
            }).many<{ key: string; label?: string; count: number }>();
        }

        return _query;
    }

    /**
     * Count number of records in table.
     * @param table name of table.
     * @param filter applied filters and filterSet to filter count.
     * @returns
     */
    count<M = Model extends undefined ? unknown : Model>(
        table: string | [string, string],
        filter?: CountQueryFilter<Model extends undefined ? M : Model>
    ) {
        const _query = concatenateQuery([
            pgFormat('SELECT COUNT(*)::INTEGER AS count FROM $<table>', {
                table: tableName(table)
            }),
            { type: 'WHERE', query: buildFilters(filter) }
        ]);

        return new Query(this.client.any, this.isBatch, pgFormat(_query), {
            command: 'RUN',
            log: this.options.query,
            table: this.table
        }).one<{ count: number }>();
    }

    /**
     * Builds INSERT query
     * @param data
     * Data object or array of data objects for INSERT query
     * @param params
     * @returns
     * Query instance with generated query as argument
     */

    add<M = Model extends undefined ? unknown : Model>(
        data: DataInput,
        params: AddQueryParams<Model extends undefined ? M : Model>
    ): Query;
    add(data: DataInput, table: string): Query;
    add<M = Model extends undefined ? unknown : Model>(
        data: DataInput,
        params: AddQueryParams<Model extends undefined ? M : Model> | string
    ) {
        const add = buildUpdateInsertQuery(
            'INSERT',
            data,
            typeof params === 'string' ? undefined : params?.columns,
            typeof params === 'string' ? params : params?.table || this.table
        );
        const query = concatenateQuery([
            add,
            {
                type: 'CONFLICT',
                query: typeof params === 'string' ? undefined : params?.conflict
            },
            {
                type: 'RETURNING',
                query: typeof params === 'string' ? '*' : params?.returning
            }
        ]);

        return new Query(this.client.any, this.isBatch, query, {
            command: 'INSERT',
            table: this.table,
            log: this.options.query
        });
    }

    /**
     * Builds UPDATE query
     * @param data
     * Data object or array of data objects for INSERT query
     * @param params
     * @returns
     * Query instance with generated query as argument
     */
    update<M = Model extends undefined ? unknown : Model>(
        data: DataInput,
        params: UpdateQueryParams<Model extends undefined ? M : Model>
    ): Query;
    update<M = Model extends undefined ? unknown : Model>(
        data: DataInput,
        table: string,
        filter: FilterInput<Model extends undefined ? M : Model>
    ): Query;
    update<M = Model extends undefined ? unknown : Model>(
        data: DataInput,
        params: string | UpdateQueryParams<Model extends undefined ? M : Model>,
        filter?: FilterInput<Model extends undefined ? M : Model>
    ): Query {
        let update = buildUpdateInsertQuery(
            'UPDATE',
            data,
            typeof params === 'string' ? undefined : params?.columns,
            typeof params === 'string' ? params : params?.table || this.table
        );

        // add WHERE for updating multiple records
        if (Array.isArray(data)) {
            update = update + ' WHERE v.id = t.id';
        }

        // filter and table depending on overload
        let _filter: string | undefined = undefined;
        const _table =
            typeof params === 'string' ? params : params?.table || this.table;

        if (filter) {
            _filter = buildFilters(filter, _table);
        } else if (typeof params !== 'string') {
            _filter = buildFilters(params?.filter, _table);
        }

        const query = concatenateQuery([
            update,
            {
                type: 'WHERE',
                query: _filter
            },
            {
                type: 'RETURNING',
                query: typeof params === 'string' ? '*' : params?.returning
            }
        ]);

        return new Query(this.client.any, this.isBatch, query, {
            command: 'UPDATE',
            table: this.table,
            log: this.options.query
        });
    }

    /**
     * Executes multiple queries with a single connection pool.
     * @param callback
     * Queries that should be executed.
     * @returns
     */
    batch<T = void>(callback: BatchQueryCallback<T>) {
        const trx = new PostgresBatchQuery(this.client, this.options);
        return trx.executeBatch<T>(callback);
    }

    /**
     * Initiates a new SQL transaction.
     * A SQL transaction is a grouping of one or more SQL statements that interact with a database.
     * A transaction in its entirety can commit to a database as a single logical unit or rollback (become undone) as a single logical unit.
     * @param callback
     * Queries that should be executed.
     * @returns
     */
    transaction<T>(callback: BatchQueryCallback<T>) {
        const trx = new PostgresBatchQuery(this.client, this.options);
        return trx.executeTransaction<T>(callback);
    }
}
