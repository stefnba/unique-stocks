import React from 'react';
import PrivateLayout from '../layout/private';
import PublicLayout from '../layout/public';

import 'antd/dist/reset.css';

import { useAppSelector, useAppDispatch } from '@redux';
import { actions } from '@features/auth/slice';

const App: React.FC = () => {
    const auth = useAppSelector((state) => state.auth);

    if (auth) {
        return <PrivateLayout />;
    }

    return <PublicLayout />;
};

export default App;
