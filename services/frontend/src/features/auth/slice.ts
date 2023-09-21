import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';

export interface AuthState {
    loggedIn: boolean;
    tokens: {
        access?: string;
    };
}

const initialState: AuthState = {
    loggedIn: false,
    tokens: {
        access: null
    }
};

export const authSlice = createSlice({
    name: 'auth',
    initialState,
    reducers: {
        login: (state) => {
            state.loggedIn = true;
        },
        logout: (state) => {
            state.loggedIn = false;
        }
    }
});

export const { actions } = authSlice;

export default authSlice.reducer;
