import pgPromise, { QueryFile } from 'pg-promise';

import type {
    QueryInput,
    QueryConcatenationParams,
    QueryInserUpdateCommands,
    ColumnsInput
} from './types';
import { QueryBuildError } from './error';
import { ColumnSet } from './column';

/**
 * Helpers for query building, e.g. insert, update
 */
export const pgHelpers = pgPromise().helpers;

/**
 * Formats, espaces and integrates parameters into query
 */
export const pgFormat = pgPromise().as.format;

/**
 * Convers QueryInput (e.g. QueryFile) to string
 * @param qf
 * Query, can either be string or QueryFile
 * @returns
 * Query converted to string
 */
export const queryToString = (qf: QueryInput): string => {
    if (qf instanceof QueryFile) {
        return pgFormat(qf);
    }
    return qf;
};

/**
 * Checks if query includes certain clauses, e.g. WHERE, RETURNING, to avoid errors
 * @param query string
 * String that will be checked if clause exists
 * @param clause string
 *
 * @returns
 */
export function queryIncludesClause(query: string, clause: string) {
    if (query.toLowerCase().includes(clause.toLowerCase())) return true;
    return false;
}

/**
 * Concatenate different parts of query and determined if certain clauses, operators need to be inserted
 * @param parts Array of string|QueryFile|object
 * - string
 * - QueryFile
 * - object with keys: type, query
 * @returns string
 * Final query with all parts concatenated
 */
export function concatenateQuery(parts: QueryConcatenationParams): string {
    let fullQuery = '';

    parts.forEach((part) => {
        if (!part) {
            return;
        }
        // normal string, QueryFile no longer possible here
        if (typeof part === 'string') {
            fullQuery += ` ${part}`;
            return;
        }
        // QueryFile
        if (part instanceof QueryFile) {
            fullQuery += ` ${pgFormat(part)}`;
            return;
        }

        // object
        const { query: q, type } = part;

        // return if undefined or empty
        if (q === undefined || queryToString(q).trim() === '') return;
        const query = pgFormat(q);

        if (type === 'RETURNING') {
            const clause = 'RETURNING';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
        if (type === 'WHERE') {
            const clause = 'WHERE';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` AND ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
        if (type === 'CONFLICT') {
            const clause = 'ON CONFLICT';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
        if (type === 'LIMIT') {
            const clause = 'LIMIT';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
        if (type === 'OFFSET') {
            const clause = 'OFFSET';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
        if (type === 'ORDER') {
            const clause = 'ORDER BY';
            if (
                queryIncludesClause(fullQuery, clause) ||
                queryIncludesClause(query, clause)
            ) {
                fullQuery += ` ${query}`;
                return;
            }
            fullQuery += ` ${clause} ${query}`;
            return;
        }
    });
    return fullQuery.trim();
}

/**
 * Simplifies INSERT or UPDATE query building based on inputs
 * @param command
 * Decides if INSERT or UPDATE to be built
 * @param data
 * Data input
 * @param columns
 * @param table
 * Table name
 * @returns
 */
export const buildUpdateInsertQuery = <M>(
    command: QueryInserUpdateCommands,
    data: object,
    columns?: ColumnsInput<M>,
    table?: string
) => {
    if (!table) {
        throw new QueryBuildError({
            message: 'A table name is required',
            type: 'TABLE_NAME_MISSING',
            command
        });
    }

    const _command = command === 'INSERT' ? 'insert' : 'update';
    const _columns = columns
        ? columns instanceof ColumnSet
            ? columns
            : new ColumnSet(columns)
        : null;

    try {
        const q = pgHelpers[_command](data, _columns, table);

        // if (q.trim() === '' || !q) {
        //     throw new QueryBuildError({
        //         message: 'Query cannot be empty',
        //         type: 'EMPTY_QUERY',
        //         command
        //     });
        // }

        return q;
    } catch (err) {
        if (err instanceof Error) {
            if (err.message.match(/Property '([a-zA-Z]+)' doesn't exist\./g)) {
                throw new QueryBuildError({
                    message: err.message,
                    type: 'DATA_PROPERTY_MISSING',
                    table,
                    column: 'rank',
                    command
                });
            }

            if (
                err.message ===
                "Parameter 'columns' is required when updating multiple records."
            ) {
                throw new QueryBuildError({
                    message: err.message,
                    type: 'COLUMNS_MISSING',
                    table,
                    column: 'rank',
                    command
                });
            }

            if (
                err.message ===
                    'Cannot generate an INSERT from an empty array.' ||
                err.message === 'Cannot generate an INSERT without any columns.'
            ) {
                throw new QueryBuildError({
                    message: `No data was provided. ${command} query cannot be generated`,
                    type: 'EMPTY_DATA',
                    table,
                    command
                });
            }
        }

        console.log('dddd', err);
    }
};
