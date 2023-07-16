import queryString from 'query-string';

import { baseApi } from '@app/api/client';

type GetFilterChoicesResult = { field: string; choices: string[] };
type GetCountResult = { count: number };
type GetFilterChoicesArgs = { field: string; filter?: object };
type GetExchangeResult = {
    id: number;

    name: string;
    lei: string;
    country_id: number;
    industry_id: number;
    description: string;
    email: string;
    jurisdiction: string;
    website: string;
    is_active: boolean;
    active_from: Date;
    active_until: Date;
    created_at: Date;
    headquarter_address_city: string;
    headquarter_address_country: string;
    headquarter_address_street: string;
    headquarter_address_street_number: string;
    headquarter_address_zip_code: string;
    legal_address_city: string;
    legal_address_country: string;
    legal_address_street: string;
    legal_address_street_number: string;
    legal_address_zip_code: string;
    sector_id: number;
    telephone: string;
    type_id: number;
    updated_at: Date;
};

const extendedApi = baseApi.injectEndpoints({
    endpoints: (build) => ({
        getAllEntity: build.query<
            GetExchangeResult[],
            { [key: string]: unknown } | void
        >({
            query: (filter) => {
                if (filter)
                    return `entity?${queryString.stringify(filter, {
                        arrayFormat: 'comma',
                        skipNull: false
                    })}`;
                return 'entity';
            }
        }),
        getOneEntity: build.query<GetExchangeResult, string>({
            query: (id) => `entity/${id}`
        })
    }),
    overrideExisting: false
});

export const { useGetOneEntityQuery, useGetAllEntityQuery } = extendedApi;
