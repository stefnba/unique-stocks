import queryString from 'query-string';

import { baseApi } from '@app/api/client';

import type {
    GetAllResult,
    GetOneResult,
    GetOneArgs,
    GetAllArgs,
    GetCountResult
} from './api.types';

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        securityGetAll: build.query<GetAllResult, GetAllArgs>({
            query: (filter) => {
                if (filter)
                    return `security?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'security';
            }
        }),
        securityGetCount: build.query<GetCountResult, GetAllArgs>({
            query: (filter) => {
                if (filter)
                    return `security/count?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'security/count';
            }
        }),
        securityGetOne: build.query<GetOneResult, GetOneArgs>({
            query: (id) => `security/${id}`
        })
    }),
    overrideExisting: false
});

export const {
    useSecurityGetAllQuery,
    useSecurityGetOneQuery,
    useSecurityGetCountQuery
} = extendedApi;
