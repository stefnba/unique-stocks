import { useSearchParams } from 'react-router-dom';
import queryString from 'query-string';
import { paginationDefault } from '@features/pagination/slice';
import type { Pagination } from '@features/pagination/slice.types';

export const usePaginationFromSearchParams = (pagination?: Pagination) => {
    let [searchParams] = useSearchParams();

    const search = queryString.parse(searchParams.toString());

    let page = null;
    let pageSize = null;

    if ('page' in search && typeof search.page === 'string') {
        try {
            page = Number.parseInt(search.page);
        } catch {}
    }
    if ('pageSize' in search && typeof search.pageSize === 'string') {
        try {
            pageSize = Number.parseInt(search.pageSize);
        } catch {}
    }

    return {
        page,
        pageSize
    };
};
