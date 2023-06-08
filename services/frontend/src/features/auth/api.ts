import { baseApi } from '@app/api/client';

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        example: build.query({
            query: () => 'exchange'
        })
    }),
    overrideExisting: false
});

export const { useExampleQuery } = extendedApi;
