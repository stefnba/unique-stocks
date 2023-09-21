import React from 'react';
import { createRoot } from 'react-dom/client';
import {
    createBrowserRouter,
    RouterProvider,
    createRoutesFromElements
} from 'react-router-dom';
import { store } from './app/redux/store';
import { Provider } from 'react-redux';
import { ConfigProvider } from 'antd';

import Routes from '@routes';

const container = document.getElementById('root');
const root = createRoot(container);
root.render(
    <React.StrictMode>
        <Provider store={store}>
            <ConfigProvider
                theme={{
                    hashed: false,
                    token: {
                        //     // colorPrimary: '#113a5d'
                        // colorPrimary: '#00b96b'
                        colorPrimary: '#ff7a8a'
                    }
                }}
            >
                <RouterProvider
                    router={createBrowserRouter(
                        createRoutesFromElements(Routes)
                    )}
                />
            </ConfigProvider>
        </Provider>
    </React.StrictMode>
);
