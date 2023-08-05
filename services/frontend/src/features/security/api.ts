import queryString from 'query-string';

import { baseApi } from '@app/api/client';

import type { GetOneSecurityResult, GetAllExchangeResult } from './api.types';

type GetFilterChoicesResult = { field: string; choices: string[] };
type GetCountResult = { count: number };
type GetFilterChoicesArgs = { field: string; filter?: object };

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        getAllSecurity: build.query<
            GetAllExchangeResult[],
            { [key: string]: unknown } | void
        >({
            query: (filter) => {
                if (filter)
                    return `security?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'security';
            }
        }),
        getOneSecurity: build.query<GetOneSecurityResult, string>({
            query: (id) => `security/${id}`
        })
    }),
    overrideExisting: false
});

export const { useGetAllSecurityQuery, useGetOneSecurityQuery } = extendedApi;
