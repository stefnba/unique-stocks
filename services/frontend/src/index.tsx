import React from 'react';
import { createRoot } from 'react-dom/client';
import {
    createBrowserRouter,
    RouterProvider,
    createRoutesFromElements
} from 'react-router-dom';
import { store } from './app/redux/store';
import { Provider } from 'react-redux';

import Routes from '@routes';

const container = document.getElementById('root');
const root = createRoot(container);
root.render(
    <React.StrictMode>
        <Provider store={store}>
            <RouterProvider
                router={createBrowserRouter(createRoutesFromElements(Routes))}
            />
        </Provider>
    </React.StrictMode>
);
