import { RootState } from '@redux';
import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react';
import config from '@config';

export const baseApi = createApi({
    baseQuery: fetchBaseQuery({
        baseUrl: config.api.baseUrl,
        prepareHeaders: (headers, { getState }) => {
            const token = (getState() as RootState).auth.tokens?.access;
            if (token) headers.set('authorization', `Bearer ${token}`);
            return headers;
        }
    }),
    endpoints: () => ({}),
    reducerPath: 'api'
});
