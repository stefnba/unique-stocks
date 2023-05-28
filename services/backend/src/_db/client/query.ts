import {
    DatabaseOptions,
    QueryExecutionCommands,
    QueryRunner,
    QueryExecutionParams
} from './types';

import {
    QueryBuildError,
    QueryExecutionError,
    QueryResultError
} from './error';

export default class Query {
    private result?: object | object[];
    /**
     * Query string that was provided or built
     */
    private query: string;
    /**
     * Pg-promise method to execute query
     */
    private queryRunner: QueryRunner;
    isBatch: boolean;
    /**
     * Name of db table if provided
     */
    private table?: string;
    private command?: QueryExecutionCommands;
    private customLog?: DatabaseOptions['query'];

    constructor(
        queryRunner: QueryRunner,
        isBatch: boolean,
        query: string,
        params: QueryExecutionParams
    ) {
        this.query = query;
        this.queryRunner = queryRunner;
        this.isBatch = isBatch;
        this.table = params.table;
        this.command = params.command;
        this.customLog = params.log;
    }

    /**
     * Executes query and returns none db record or throws Error
     * @returns
     * One db record or null
     */
    async one<R>(): Promise<R> {
        const result = await this.getResult();

        if (result.length === 0 || !result) {
            throw new QueryResultError({
                table: this.table,
                command: this.command,
                type: 'RECORD_NOT_FOUND',
                message: 'Record does not exist',
                query: this.query
            });
        }
        if (result.length > 1) {
            throw new QueryResultError({
                table: this.table,
                command: this.command,
                type: 'ONE_RECORD_VIOLATION',
                message: 'Multiple records not allowed',
                query: this.query
            });
        }
        return result[0];
    }

    /**
     * Executes query and returns either none db record or none
     * @returns
     * One db record or null
     */
    async oneOrNone<R>(): Promise<R | null> {
        const result = await this.getResult();

        if (result.length > 1) {
            throw new QueryResultError({
                table: this.table,
                command: this.command,
                type: 'ONE_RECORD_VIOLATION',
                message: 'Multiple records not allowed',
                query: this.query
            });
        }
        if (result.length === 0 || !result) {
            return null;
        }
        return result[0];
    }

    /**
     * Executes query and expects to return many records. If no record found, throws QueryResultError
     * @returns
     * Array of db records
     */
    async manyStrict<R>(): Promise<R[]> {
        const result = await this.getResult();
        if (result.length === 0 || !result) {
            throw new QueryResultError({
                table: this.table,
                command: this.command,
                type: 'RECORD_NOT_FOUND',
                message: 'Records do not exist',
                query: this.query
            });
        }
        return result;
    }

    /**
     * Executes query and returns many db records
     * @returns
     * Array of db records
     */
    many<R>(): Promise<R[]> {
        return this.getResult();
    }

    /**
     * Executes query but does not return any records
     */
    async none(): Promise<void> {
        await this.getResult();
    }

    /**
     * Decides whether to execute query or used cached results from previously executed query
     * @returns Array<object> | object
     * Result, either
     */
    private async getResult() {
        if (this.result) return this.result;
        const result = await this.execute();
        this.result = result;
        return result;
    }

    /**
     * Triggers query runner to execute query and return results.
     * Also handle errors
     * @returns
     * Result from query
     */
    private async execute() {
        if (!this.query || this.query.trim() === '') {
            throw new QueryBuildError({
                message: 'Query cannot be empty',
                type: 'EMPTY_QUERY',
                query: this.query,
                command: this.command
            });
        }

        // console.log('executed');
        return this.queryRunner(this.query)
            .then((r) => {
                if (this.customLog?.onReturn) {
                    this.customLog?.onReturn(r, this.query);
                }
                return r;
            })
            .catch((err) => {
                if (this.customLog?.onError) {
                    this.customLog?.onError(
                        { message: err.message },
                        this.query
                    );
                } else {
                    throw new QueryExecutionError({
                        command: this.command,
                        message: err.message,
                        query: this.query,
                        table: this.table,
                        cause: err
                    });
                }
                return err;
            });
    }
}
