import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';

export interface LogState {
    filtering: {
        applied: { [key: string]: any };
    };
}

const initialState: LogState = {
    filtering: {
        applied: {}
    }
};

export const slice = createSlice({
    name: 'log',
    initialState,
    reducers: {
        applyFilter: (state, action: PayloadAction<{ [key: string]: any }>) => {
            state.filtering.applied = action.payload;
        }
    }
});

export const { actions } = slice;

export default slice.reducer;
