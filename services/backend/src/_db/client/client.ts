import pgPromise from 'pg-promise';
import chalk from 'chalk';

import QueryBuilder from './builder';

import type {
    DatabaseConnectionParams,
    DatabaseConnectionStatus,
    RegisteredRepositories,
    DatabaseClient,
    DatabaseOptions,
    RepositoriesParams,
    DatabaseClientExtended
} from './types';
import { ConnectionError } from './error';
import { sqlFile } from './queryFile';
import { applyFilter } from './filter';
import { ColumnSet } from './column';

export default class PostgresClient {
    db: DatabaseClient;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    repos?: RegisteredRepositories<any>;
    readonly query: QueryBuilder;
    private connectionStatus: DatabaseConnectionStatus;
    private options: DatabaseOptions;

    constructor(
        connection: DatabaseConnectionParams,
        options: DatabaseOptions = {}
    ) {
        const pgp = pgPromise({
            capSQL: true,
            noWarnings: options.noWarnings || false
        });
        this.db = pgp(connection);

        this.options = {
            ...options,
            connect: {
                testOnInit: true,
                log: true,
                ...options.connect
            },
            noWarnings: false
        };

        this.connectionStatus = {
            status: 'DISCONNECTED',
            serverVersion: undefined,
            connection: {
                host: connection.host,
                port: connection.port,
                database: connection.database,
                user: connection.user,
                password: '##########' // hide password
            }
        };

        this.query = this.queryInit();

        // test if connection can be established
        if (this.options.connect?.testOnInit) {
            (async () => {
                await this.connect();
            })();
        }
    }

    /**
     * Initiates QueryBuilder for client
     * @param table string
     * Define table for logging, add and update methods
     * @returns
     * New QueryBuilder instance
     */
    private queryInit(table?: string) {
        return new QueryBuilder(this.db, this.options, table);
    }

    /**
     * Terminates db client and open connection
     */
    async close() {
        return this.db.$pool.end();
        // await this.db.
    }

    /**
     * Attempts to establish connection to database
     * @returns
     */
    async connect() {
        return this.db
            .connect()
            .then((c) => {
                c.done(true);
                this.connectionStatus.status = 'CONNECTED';
                this.connectionStatus.serverVersion = c.client.serverVersion;

                // log or return onSuccess
                if (this.options?.connect?.onSuccess) {
                    this.options?.connect?.onSuccess(this.connectionStatus);
                } else if (this.options?.connect?.log) {
                    const { host, database, user, port } =
                        this.connectionStatus.connection;
                    console.log(
                        chalk.green(
                            `Connected to Database (version: ${c.client.serverVersion}) "${database}" on ${host}:${port} with user "${user}"`
                        )
                    );
                }
                return this.status();
            })
            .catch((err) => {
                if (!err.code) throw err;

                const error = new ConnectionError({
                    connection: this.connectionStatus.connection,
                    message: '',
                    cause: err
                });

                // status
                this.connectionStatus = {
                    ...this.connectionStatus,
                    status: 'FAILED',
                    error: error.public()
                };

                // log or return onFailed
                if (this.options?.connect?.onFailed) {
                    this.options?.connect?.onFailed(this.connectionStatus);
                } else if (this.options?.connect?.log) {
                    const { host, database, user, port } =
                        this.connectionStatus.connection;
                    console.error(
                        chalk.red(`DB connection failed (${err.message})`)
                    );
                    console.error(`Host\t\t${host}`);
                    console.error(`Port\t\t${port}`);
                    console.error(`Database\t${database}`);
                    console.error(`User\t\t${user}`);
                }

                return this.status();
            });
    }

    /**
     * Returns connection status of database
     * @returns
     * Status of database connection
     */
    async status(): Promise<DatabaseConnectionStatus> {
        if (this.connectionStatus.status === 'DISCONNECTED')
            return this.connect();
        return this.connectionStatus;
    }

    /**
     * Extends PostgresClient with custom respositories that can be used throughout the application
     * @param databaseRespos
     * Respositories as key-value pairs
     * @returns
     * Extended DatabaseClient with initialized Repositories for this.repo property
     */
    addRepositories<T extends RepositoriesParams>(
        databaseRespos: T
    ): DatabaseClientExtended<T> {
        const repos = Object.entries(databaseRespos).reduce(
            (acc, [key, Repo]) => {
                const repo = new Repo();

                // attach query
                repo.query = this.queryInit(repo.table);

                return {
                    ...acc,
                    [key]: repo
                };
            },
            {}
        ) as RegisteredRepositories<T>;

        this.repos = repos;

        return this as DatabaseClientExtended<T>;
    }

    /**
     * Helper methods that are exposed for convenience purpose
     */
    helpers = {
        sqlFile: sqlFile,
        filter: applyFilter,
        ColumnSet: ColumnSet
    };
}
