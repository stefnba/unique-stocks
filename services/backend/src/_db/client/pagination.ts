import type { PaginationInput } from './types';
import { pgFormat } from './utils';

const defaultPageSize = 25;

/**
 * Translates pagination params into Postgres LIMIT statements
 * @param pagination which page and how many records to show
 * @returns Query that can be integrated in run statement with LIMIT and OFFSET
 */
const pageSize = (pagination: PaginationInput | undefined) => {
    if (!pagination) return;
    const { pageSize = defaultPageSize } = pagination;

    return pgFormat('LIMIT $<pageSize>', { pageSize });
};

/**
 * Translates pagination params into Postgres OFFSET statements
 * @param pagination which page and how many records to show
 * @returns Query that can be integrated in run statement with LIMIT and OFFSET
 */
const page = (pagination: PaginationInput | undefined) => {
    if (!pagination) return;
    const { page = 1, pageSize = defaultPageSize } = pagination;

    const offset = page > 0 ? (page - 1) * pageSize : 0;
    return pgFormat('OFFSET $<offset>', { offset });
};

export default {
    pageSize,
    page
};
