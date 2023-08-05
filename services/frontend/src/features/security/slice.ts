import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';
import type { LogState } from './slice.types';

const initialState: LogState = {
    filtering: {
        applied: {}
    },
    pagination: {
        page: 1,
        pageSize: 50
    }
};

export const slice = createSlice({
    name: 'security',
    initialState,
    reducers: {
        applyFilter: (state, action: PayloadAction<{ [key: string]: any }>) => {
            state.filtering.applied = action.payload;
        },
        changePagination: (
            state,
            action: PayloadAction<{
                page: number;
                pageSize: number;
            }>
        ) => {
            state.pagination = {
                page: action.payload.page,
                pageSize: action.payload.pageSize
            };
        }
    }
});

export const { actions } = slice;

export default slice.reducer;
