import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';
import type { LogState, FilteringActionPayload } from './slice.types';

const initialState: LogState = {
    entity: {},
    entitySecurity: {},
    exchange: {},
    exchangeSecurity: {},
    security: {},
    securityListing: {}
};

export const slice = createSlice({
    name: 'filtering',
    initialState,
    reducers: {
        apply: (state, action: PayloadAction<FilteringActionPayload>) => {
            const { component, filters } = action.payload;

            return {
                ...state,
                [component]: filters
            };
        }
    }
});

export const { actions } = slice;

export default slice.reducer;
