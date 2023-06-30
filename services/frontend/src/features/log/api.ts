import queryString from 'query-string';

import { baseApi } from '@app/api/client';

type GetFilterChoicesResult = { field: string; choices: string[] };
type GetFilterChoicesArgs = { field: string; filter?: object };

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        /**
         * Also apply filters for logs.
         */
        getAll: build.query<any[], { [key: string]: any } | void>({
            query: (filter) => {
                if (filter)
                    return `log?${queryString.stringify(filter, {
                        arrayFormat: 'comma'
                    })}`;
                return 'log';
            }
        }),
        getOne: build.query<object, string>({
            query: (id) => `log/${id}`
        }),
        getFilterChoices: build.query<
            GetFilterChoicesResult,
            GetFilterChoicesArgs
        >({
            query: ({ field, filter }) => {
                console.log(filter);
                if (Object.keys(filter).length > 0)
                    return `log/${field}/choices?${queryString.stringify(
                        filter,
                        {
                            arrayFormat: 'comma'
                        }
                    )}`;
                return `log/${field}/choices`;
            }

            // transformResponse: (response) => {
            //     return response;
            // }
        })
    }),
    overrideExisting: false
});

export const { useGetAllQuery, useGetOneQuery, useGetFilterChoicesQuery } =
    extendedApi;
