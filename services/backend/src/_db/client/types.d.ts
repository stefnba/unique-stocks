import {
    IDatabase,
    ITask,
    QueryFile,
    IColumnConfig,
    IBaseProtocol
} from 'pg-promise';

import {
    ConnectionError,
    QueryBuildError,
    QueryExecutionError,
    QueryResultError
} from './error';
import QueryBuilder from './builder';
import { filterOperators } from './filter';
import DatabaseRepository from './repository';
import { ColumnSet } from './column';
import PostgresClient from './client';

export type DatabaseClient = IDatabase<object>;
export type DatabaseClientExtended<T extends RepositoriesParams> =
    PostgresClient & {
        repos: RegisteredRepositories<T>;
    };

export type DatabaseConnectionParams = {
    host?: string;
    port?: number;
    database: string;
    user: string;
    password: string;
};

export type DatabaseConnectionStatus = {
    status: 'CONNECTED' | 'FAILED' | 'DISCONNECTED';
    serverVersion?: string;
    connection: DatabaseConnectionParams;
    error?: ConnectionErrorPublic;
};

export type ConnectionEventSuccessParams = Omit<
    DatabaseConnectionStatus,
    'error'
>;
export type ConnectionEventFailParams = Omit<
    DatabaseConnectionStatus,
    'serverVersion'
>;

export type DatabaseOptions = {
    connect?: {
        testOnInit?: boolean;
        log?: boolean;
        onSuccess?: (connection: ConnectionEventSuccessParams) => void;
        onFailed?: (connection: ConnectionEventFailParams) => void;
    };
    query?: {
        onError?: (error: { message: string }, query: string) => void;
        onReturn?: (result: Array<object> | object, query: string) => void;
    };
    transaction?: {
        onBegin?: () => void;
        onCommmit?: () => void;
        onRollback?: (
            error: QueryBuildError | QueryExecutionError | QueryResultError
        ) => void;
    };
    noWarnings?: boolean;
};

// Repositories

export type RepositoriesParams = Record<
    string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    typeof DatabaseRepository<any>
>;
export type RegisteredRepositories<R extends RepositoriesParams> = {
    [Key in keyof R]: Repository<InstanceType<R[Key]>>;
};
export type Repository<R> = Omit<R, 'table' | 'query' | 'sqlFilesDir'>;

// Errors
export type QueryErrorArgs = {
    message: string;
    table?: string;
    command?: QueryExecutionCommands;
    hint?: string;
    query?: string;
};

export type QueryExecutionErrorArgs = QueryErrorArgs & {
    cause: PostgresErrorObject;
};

export type ConnectionErrorArgs = {
    connection: DatabaseConnectionParams;
    message: string;
    cause: PostgresErrorObject;
};

export type QueryBuildErrorParams = QueryErrorArgs & {
    type:
        | 'EMPTY_DATA'
        | 'TABLE_NAME_MISSING'
        | 'COLUMNS_MISSING'
        | 'EMPTY_QUERY'
        | 'DATA_PROPERTY_MISSING'
        | 'SQL_FILE_NOT_FOUND';
    column?: string;
};

export type QueryResultErrorParams = QueryErrorArgs & {
    type: 'ONE_RECORD_VIOLATION' | 'RECORD_NOT_FOUND';
};

export type ConnectionErrorPublic = Pick<
    ConnectionError,
    'code' | 'message' | 'type'
>;

export type PostgresErrorObject = Error & {
    code: string;
    detail?: string;
    hint?: string;
    length?: number;
    severity?: string;
    schema?: string;
    table?: string;
    column?: string;
    constraint?: string;
    dataType?: string;
    routine?: string;
    query?: string;
};

// Query
export type QueryRunner = IBaseProtocol<unknown>['any'];
export type QueryExecutionParams = {
    table?: string;
    command?: QueryExecutionCommands;
    log?: DatabaseOptions['query'];
};

export type QueryInput = string | QueryFile;
export type QueryCommands = 'SELECT' | 'UPDATE' | 'INSERT';
export type QueryExecutionCommands = QueryCommands | 'RUN';
export type QueryInserUpdateCommands = 'UPDATE' | 'INSERT';
export type QueryClauses =
    | 'WHERE'
    | 'RETURNING'
    | 'CONFLICT'
    | 'LIMIT'
    | 'OFFSET'
    | 'ORDER';

export type QueryConcatenationParams = Array<
    | QueryInput
    | {
          type: QueryClauses;
          query?: QueryInput;
      }
>;

export type BatchQuery = <R>(
    callback: BatchQueryCallback<R>
) => unknown | Promise<unknown>;

export type BatchClient = ITask<object>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type BatchQueryCallback<R = any> = (t: QueryBuilder) => Promise<R>;

export type FindQueryParams<M> = {
    params?: object;
    filter?: FilterInput<M>;
    pagination?: PaginationInput;
};

export type PaginationInput = {
    page?: number;
    pageSize?: number;
};

export type AddQueryParams<M> = {
    columns?: ColumnsInput<M>;
    returning?: QueryInput;
    table?: string;
    conflict?: string;
};

export type UpdateQueryParams<M> = Omit<AddQueryParams<M>, 'conflict'> &
    Pick<FindQueryParams<M>, 'filter'>;

export type DataInput = object | object[];
export type ColumnsInput<M> = ColumnSetParams<M> | ColumnSet<M>;

// Filters
export type FilterInput<M> =
    | string
    | { filter: object | undefined; filterSet: FilterSet<M> };
export type FilterOperators = keyof typeof filterOperators;
export type FilterOperatorParams = {
    column: string | number | symbol;
    value: unknown;
    alias?: string;
};

export type FilterSet<M = undefined> = Record<
    string,
    | FilterOperators
    | {
          column: M extends undefined ? string : keyof M;
          operator: FilterOperators;
          alias?: string;
      }
>;

export type ColumnSetParams<M = undefined> = M extends undefined
    ? Array<string | ({ name: string; optional?: boolean } & IColumnConfig<M>)>
    : Array<
          | ({ name: keyof M; optional?: boolean } & IColumnConfig<M>)
          | keyof M
          | `${string & keyof M}?`
      >;
