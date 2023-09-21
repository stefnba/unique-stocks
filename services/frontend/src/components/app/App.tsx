import React from 'react';
import PrivateLayout from '../layout/Private';
import PublicLayout from '../layout/Public';

import '../../style/app.css';
import 'antd/dist/reset.css';

import { useAppSelector } from '@redux';

const App: React.FC = () => {
    const auth = useAppSelector((state) => state.auth);

    if (auth) {
        return <PrivateLayout />;
    }

    return <PublicLayout />;
};

export default App;
