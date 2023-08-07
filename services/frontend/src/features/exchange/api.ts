import queryString from 'query-string';

import { baseApi } from '@app/api/client';

import type {
    GetOneExchangeResult,
    ExchangeGetSecurityResult
} from './api.types';

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
        exchangeGetAll: build.query<
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
        exchangeGetCount: build.query<
            GetCountResult,
            { [key: string]: unknown } | void
        >({
            query: (filter) => {
                if (filter)
                    return `exchange/count?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'exchange/count';
            }
        }),
        exchangeGetOne: build.query<GetOneExchangeResult, string>({
            query: (id) => `exchange/${id}`
        }),
        exchangeGetSecurity: build.query<ExchangeGetSecurityResult, string>({
            query: (id) => `exchange/${id}/security`
        }),
        exchangeGetSecurityCount: build.query<GetCountResult, string>({
            query: (id) => `exchange/${id}/security/count`
        })
    }),
    overrideExisting: false
});

export const {
    useExchangeGetSecurityCountQuery,
    useExchangeGetAllQuery,
    useExchangeGetOneQuery,
    useExchangeGetCountQuery,
    useExchangeGetSecurityQuery
} = extendedApi;
