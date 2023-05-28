import type {
    QueryErrorArgs,
    QueryExecutionErrorArgs,
    ConnectionErrorArgs,
    DatabaseConnectionParams,
    PostgresErrorObject,
    ConnectionErrorPublic,
    QueryBuildErrorParams,
    QueryResultErrorParams,
    QueryExecutionCommands
} from './types';

import { pgErrorCodes } from './constants';

/**
 * Query Error that extends Error with additional properties
 */
export class QueryError extends Error {
    table?: string;
    query?: string;
    command?: QueryExecutionCommands;

    constructor(params: QueryErrorArgs) {
        const { message, table, query, command } = params;
        super(message);

        this.message = message;
        this.table = table;
        this.query = query;
        this.command = command;

        // Object.setPrototypeOf(this, new.target.prototype);
        // Error.captureStackTrace(this, this.constructor);
    }
}

export class QueryResultError extends QueryError {
    type?: QueryResultErrorParams['type'];

    constructor(params: QueryResultErrorParams) {
        const { message, type, command, query } = params;
        super({ message, command, query });

        this.type = type;
    }
}

export class QueryBuildError extends QueryError {
    type?: QueryBuildErrorParams['type'];

    constructor(params: QueryBuildErrorParams) {
        const { message, type, command, query, table } = params;
        super({ message, command, table });

        this.type = type;
        this.query = query;
    }
}

export class QueryExecutionError extends QueryError {
    code: string;
    type?: string;
    column?: string;
    detail?: string;
    schema?: string;
    constraint?: string;
    private cause: PostgresErrorObject;

    constructor(params: QueryExecutionErrorArgs) {
        const { message, cause, command, query, table } = params;
        super({ message, command });

        const code = cause.code;
        this.code = code;

        this.column = cause.column;
        this.detail = cause.detail;
        this.schema = cause.schema;
        this.table = cause.table || table;
        this.constraint = cause.constraint;
        this.query = cause.query || query;
        this.cause = cause;

        if (code && code in pgErrorCodes) {
            this.type =
                pgErrorCodes[code as keyof typeof pgErrorCodes].toUpperCase();
        }
        // console.log(this);
    }
}

/**
 * Error related to failed connections
 */
export class ConnectionError extends Error {
    connection: DatabaseConnectionParams;
    private cause: PostgresErrorObject;
    code: string;
    type!:
        | 'AuthFailed'
        | 'DatabaseNotFound'
        | 'HostNotFound'
        | 'PortNotResponding'
        | 'RoleNotFound';

    constructor({ connection, message, cause }: ConnectionErrorArgs) {
        super(message);
        this.connection = connection;
        this.cause = cause;

        this.code = cause.code;
        if (cause.code) {
            if (cause.code === 'ENOTFOUND' || cause.code === 'EAI_AGAIN')
                this.hostNotFound();
            if (cause.code === 'ECONNREFUSED') this.PortNotResponding();
            if (cause.code === '3D000') this.dbNotFound();
            if (cause.code === '28P01') this.authFailed();
            if (cause.code === '28000') this.roleNotFound();
        }
    }

    public(): ConnectionErrorPublic {
        return {
            type: this.type,
            code: this.code,
            message: this.message
        };
    }
    private hostNotFound() {
        this.type = 'HostNotFound';
        this.message = `Connection to the host "${this.connection.host}" could not be established`;
    }
    private PortNotResponding() {
        this.type = 'PortNotResponding';
        this.message = `Connection to port "${this.connection.port}" on host "${this.connection.host}" refused`;
    }
    private authFailed() {
        this.type = 'AuthFailed';
        this.message = `Authentication for user "${this.connection.user}" failed`;
    }
    private dbNotFound() {
        this.type = 'DatabaseNotFound';
        this.message = `Database "${this.connection.database}" not found`;
    }
    private roleNotFound() {
        this.type = 'RoleNotFound';
        this.message = `Role "${this.connection.user}" not found`;
    }
}
