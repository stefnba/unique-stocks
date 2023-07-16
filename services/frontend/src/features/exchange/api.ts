import queryString from 'query-string';

import { baseApi } from '@app/api/client';

type GetFilterChoicesResult = { field: string; choices: string[] };
type GetCountResult = { count: number };
type GetFilterChoicesArgs = { field: string; filter?: object };
type GetExchangeResult = {
    id: number;
    operating_exchange_id: number;
    mic: string;
    name: string;
    country_id: number;
    currency: string;
    website: string;
    timezone: string;
    comment: string;
    acronym: string;
    status: string;
    source: string;
    is_active: boolean;
    is_virtual: boolean;
};

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        getAllExchange: build.query<
            GetExchangeResult[],
            { [key: string]: unknown } | void
        >({
            query: (filter) => {
                if (filter)
                    return `exchange?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'exchange';
            }
        }),
        getOneExchange: build.query<GetExchangeResult, string>({
            query: (id) => `exchange/${id}`
        })
    }),
    overrideExisting: false
});

export const { useGetAllExchangeQuery, useGetOneExchangeQuery } = extendedApi;
