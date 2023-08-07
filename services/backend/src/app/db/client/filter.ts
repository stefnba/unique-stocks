import { pgFormat } from './utils.js';

import type { FilterInput, FilterOperatorParams, FilterSet } from './types.js';

const columnAlias = (column: string | number | symbol, alias?: string) => {
    if (alias) {
        return pgFormat('$<alias:name>.$<column:name> ', { column, alias });
    }
    return pgFormat('$<column:name> ', { column });
};

export const filterOperators = {
    NULL: () => 'IS NULL',
    NOT_NULL: () => 'IS NOT NULL',
    INCLUDES: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) + pgFormat('IN ($<value:list>)', { value }),
    /**
     * Helper to filter for IS NULL OR IS NOT NULL conditions, similar to IN LIST filter
     */
    INCLUDES_NULL: ({ column, alias, value }: FilterOperatorParams) => {
        if (Array.isArray(value) && value.length === 2) {
            const conditions = value.map((v) => {
                if (v === 'NULL') return columnAlias(column, alias) + 'IS NULL';
                if (v === 'NOT_NULL')
                    return columnAlias(column, alias) + 'IS NOT NULL';
                return;
            });

            return (
                '(' +
                conditions.filter((i) => i !== undefined).join(' OR ') +
                ')'
            );
        }
        if (!Array.isArray(value) && typeof value === 'string') {
            if (value === 'NULL') return columnAlias(column, alias) + 'IS NULL';
            if (value === 'NOT_NULL')
                return columnAlias(column, alias) + 'IS NOT NULL';
            return '';
        }
    },
    EXCLUDES: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('NOT IN ($<value:list>)', {
            value
        }),
    EQUAL: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('= $<value>', {
            column,
            value,
            alias
        }),
    NOT_EQUAL: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('!= $<value>', {
            column,
            value,
            alias
        }),
    GREATER_EQUAL: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('>= $<value>', {
            column,
            value,
            alias
        }),
    GREATER: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('> $<value>', {
            column,
            value,
            alias
        }),
    LESS_EQUAL: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('<= $<value>', {
            column,
            value,
            alias
        }),
    LESS: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat('< $<value>', {
            column,
            value,
            alias
        }),
    LIKE: ({ column, alias, value }: FilterOperatorParams) =>
        columnAlias(column, alias) +
        pgFormat("LIKE LOWER('%$<value:value>%')", { value })
};

/**
 * Handler function or constructing filter string in case object was provided. Calls applyFilter function
 * @param filter
 * @param table
 * @returns
 * Filter string from applyFilter()
 */
export const buildFilters = (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    filter?: FilterInput<any>,
    table?: string
): string | undefined => {
    if (!filter) return undefined;
    if (typeof filter === 'string') return filter;

    return applyFilter(filter.filter, filter?.filterSet, table);
};

/**
 *
 * @param filters
 * @param filterSet
 * @param table
 * @returns string
 * Filter string
 */
export const applyFilter = (
    filters: object | undefined,
    filterSet: FilterSet,
    table?: string
): string => {
    if (!filters) return '';
    const appliedFilters = Object.entries(filters).map(
        ([filterKey, filterValue]) => {
            if (!(filterKey in filterSet)) return;

            // filters with value undefined are ignored
            if (filterValue === undefined) {
                return;
            }

            const filterConfig = filterSet[filterKey];
            if (filterConfig) {
                let sql: string;

                // shorthand version, i.e. filterKey is key ob object, operator is value
                if (typeof filterConfig === 'string') {
                    sql = filterOperators[filterConfig]({
                        alias: table,
                        column: filterKey,
                        value: filterValue
                    });
                } else {
                    sql = filterOperators[filterConfig.operator]({
                        alias: filterConfig.alias || table,
                        column: filterConfig.column,
                        value: filterValue
                    });
                }

                return {
                    filter: filterKey,
                    value: filterValue,
                    operator:
                        typeof filterConfig === 'string'
                            ? filterConfig
                            : filterConfig.operator,
                    sql
                };
            }
        }
    );

    if (appliedFilters.length === 0) return '';
    return appliedFilters
        .filter((f) => f !== undefined)
        .map((f) => f?.sql)
        .join(' AND ');
};
