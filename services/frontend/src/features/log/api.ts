import queryString from 'query-string';

import { baseApi } from '@app/api/client';

type GetFilterChoicesResult = { field: string; choices: string[] };
type GetCountResult = { count: number };
type GetFilterChoicesArgs = { field: string; filter?: object };
type GetLogResult = {
    name?: string;
    levelname?: string;
    created?: string;
    service?: string;
    filename: string;
    pathname: string;
    funcName: string;
    module: string;
    message: string;
    dag_id: string;
    run_id: string;
    task_id: string;
    event: string;
    extra: object;
};

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        /**
         * Also apply filters for logs.
         */
        getAll: build.query<any[], { [key: string]: unknown } | void>({
            query: (filter) => {
                if (filter)
                    return `log?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'log';
            }
            // keepUnusedDataFor: 0
        }),
        getOne: build.query<GetLogResult, string>({
            query: (id) => `log/${id}`
        }),
        getCount: build.query<
            GetCountResult,
            { [key: string]: unknown } | void
        >({
            query: (filter) => {
                if (filter)
                    return `log/count?${queryString.stringify(filter, {
                        arrayFormat: 'comma'
                    })}`;
                return 'log/count';
            }
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

export const {
    useGetAllQuery,
    useGetOneQuery,
    useGetFilterChoicesQuery,
    useGetCountQuery
} = extendedApi;
