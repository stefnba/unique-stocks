import { configureStore } from '@reduxjs/toolkit';

import authReducer from '../../features/auth/slice';
import logReducer from '../../features/log/slice';
import paginationReducer from '../../features/pagination/slice';
import filteringReducer from '../../features/filtering/slice';
import { baseApi } from '../api/client';

export const store = configureStore({
    reducer: {
        auth: authReducer,
        pagination: paginationReducer,
        filtering: filteringReducer,
        log: logReducer,
        [baseApi.reducerPath]: baseApi.reducer
    },
    middleware: (getDefaultMiddleware) =>
        getDefaultMiddleware().concat(baseApi.middleware)
});

// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>;
// Inferred type: {posts: PostsState, comments: CommentsState, users: UsersState}
export type AppDispatch = typeof store.dispatch;
