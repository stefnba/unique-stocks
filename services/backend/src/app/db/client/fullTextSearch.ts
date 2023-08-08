import type { FindQueryParams, FilterSet } from './types.js';
import { sqlFile } from './queryFile.js';
import { fileDirName } from '@root/shared/utils/module.js';
import { pgFormat, tableName } from './utils.js';
import { buildFilters } from './filter.js';

const DEFAULT_SIMILARITY_THRESHOLD = 0.05;

const ftsQuery = sqlFile(
    './sql/fullTextSearch.sql',
    fileDirName(import.meta).__dirname
);

export default class FullTextSearch {
    private fullTextSearch: FindQueryParams<undefined>['search'];
    private filter: FindQueryParams<undefined>['filter'];
    search: {
        field: string;
        column: string;
        searchQuery: string;
    };

    constructor(
        fullTextSearch: FindQueryParams<undefined>['search'],
        filter: FindQueryParams<undefined>['filter']
    ) {
        this.fullTextSearch = fullTextSearch;
        this.filter = filter;
    }

    /**
     * Check if the current query should be converted into FullTextSearch query.
     */
    hasFullTextSearchQuery() {
        const fullTextSearch = this.fullTextSearch;
        const filter = this.filter;

        // if no fullTextSearch specifed
        if (!fullTextSearch || typeof filter === 'string') return false;

        // find filterSets with FULL_TEXT_SEARCH operator
        const filterSetsWithFts = Object.entries(filter.filterSet)
            .map(([field, fs]) => {
                if (
                    typeof fs === 'string'
                        ? fs === 'FULL_TEXT_SEARCH'
                        : fs.operator === 'FULL_TEXT_SEARCH'
                ) {
                    return field;
                }
            })
            .filter((v) => v);

        // no filterSets with FULL_TEXT_SEARCH operator found
        if (filterSetsWithFts.length === 0) return false;

        const fullTextSearchFilter = filterSetsWithFts
            .map((fs) => {
                if (fs in filter.filter) {
                    const filterSet = filter.filterSet[fs];
                    return {
                        field: fs,
                        searchQuery: String(filter.filter[fs]),
                        column:
                            typeof filterSet === 'string'
                                ? fs
                                : filterSet.column
                    };
                }
            })
            .filter((i) => i);

        // no selected filter with FULL_TEXT_SEARCH filterSets operator found
        if (fullTextSearchFilter.length === 0) return false;

        // assing query and column to be used in this.buildQuery()
        this.search = fullTextSearchFilter[0];

        return true;
    }

    /**
     * Construt the FullTextSearch query based on the searchQuery and the inital query.
     * @param searchQuery Term to search against.
     * @param query Initial SQL query.
     * @returns
     */
    buildQuery(originalQuery: string) {
        const fullTextSearch = this.fullTextSearch;
        const filter = buildFilters(this.filter);
        const search = this.search;

        return pgFormat(ftsQuery, {
            searchQuery: search.searchQuery,
            table: tableName(fullTextSearch.table),
            originalQuery,
            tokenColumn: this.search.column,
            joinColumnOriginalQuery:
                typeof fullTextSearch.joinColumns === 'string'
                    ? fullTextSearch.joinColumns
                    : fullTextSearch.joinColumns.query,
            joinColumnFtsQuery:
                typeof fullTextSearch.joinColumns === 'string'
                    ? fullTextSearch.joinColumns
                    : fullTextSearch.joinColumns.search,
            filter: filter.length > 0 ? filter + ' AND ' : '',
            similarityThreshold:
                fullTextSearch.similarityThreshold ||
                DEFAULT_SIMILARITY_THRESHOLD
        });
    }
}
