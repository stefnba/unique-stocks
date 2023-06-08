import { configureStore } from '@reduxjs/toolkit';
import counterReducer from '../../features/counter/slice';
import authReducer from '../../features/auth/slice';
import { baseApi } from '../api/client';

export const store = configureStore({
    reducer: {
        counter: counterReducer,
        auth: authReducer,
        [baseApi.reducerPath]: baseApi.reducer
    },
    middleware: (getDefaultMiddleware) =>
        getDefaultMiddleware().concat(baseApi.middleware)
});

// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>;
// Inferred type: {posts: PostsState, comments: CommentsState, users: UsersState}
export type AppDispatch = typeof store.dispatch;
