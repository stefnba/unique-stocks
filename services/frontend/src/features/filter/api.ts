import queryString from 'query-string';

import { baseApi } from '@app/api/client';

import type { FilterGetSelectChoicesResult } from './api.types';

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        filterGetSelectChoices: build.query<
            FilterGetSelectChoicesResult,
            string
        >({
            query: (endpoint) => {
                return endpoint;
            }
        })
    }),
    overrideExisting: false
});

export const { useFilterGetSelectChoicesQuery } = extendedApi;
