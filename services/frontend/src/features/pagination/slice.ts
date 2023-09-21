import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';
import type { LogState, Pagination, PaginationComponents } from './slice.types';

export const paginationDefault: Pagination = {
    page: 1,
    pageSize: 20
};

const initialState: LogState = {
    entity: paginationDefault,
    entitySecurity: paginationDefault,
    exchange: paginationDefault,
    exchangeSecurity: paginationDefault,
    security: paginationDefault,
    securityListing: paginationDefault
};

export const slice = createSlice({
    name: 'pagination',
    initialState,
    reducers: {
        change: (
            state,
            action: PayloadAction<
                { component: PaginationComponents } & Pagination
            >
        ) => {
            const { component, page, pageSize } = action.payload;

            return {
                ...state,
                [component]: {
                    page,
                    pageSize
                }
            };
        }
    }
});

export const { actions } = slice;

export default slice.reducer;
