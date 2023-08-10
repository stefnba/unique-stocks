import queryString from 'query-string';

import { baseApi } from '@app/api/client';

import type {
    GetAllResult,
    GetOneResult,
    GetOneArgs,
    GetAllArgs,
    GetCountResult,
    GetListingArgs,
    GetListingResult
} from './api.types';

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        securityGetAll: build.query<GetAllResult, GetAllArgs>({
            query: (filter) => {
                if (filter)
                    return `security?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: true
                    })}`;
                return 'security';
            }
        }),
        securityGetListing: build.query<GetListingResult, GetListingArgs>({
            query: ({ id, filters }) => {
                if (filters)
                    return `security/${id}/listing?${queryString.stringify(
                        filters,
                        {
                            arrayFormat: 'comma',
                            skipNull: true
                        }
                    )}`;
                return `security/${id}/listing`;
            }
        }),
        securityGetCount: build.query<GetCountResult, GetAllArgs>({
            query: (filter) => {
                if (filter)
                    return `security/count?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: true
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
    useSecurityGetCountQuery,
    useSecurityGetListingQuery
} = extendedApi;
