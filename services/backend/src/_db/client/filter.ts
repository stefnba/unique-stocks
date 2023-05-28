import { pgFormat } from './utils';

import type { FilterInput, FilterOperatorParams, FilterSet } from './types';

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
            if (filterKey in filterSet) {
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
            return;
        }
    );
    if (appliedFilters.length === 0) return '';
    return appliedFilters
        .filter((f) => f !== undefined)
        .map((f) => f?.sql)
        .join(' AND ');
};
