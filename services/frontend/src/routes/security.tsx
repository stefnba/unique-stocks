import SecurityList from '@components/security/List';

import { Route } from 'react-router-dom';

export default (
    <Route path="/security">
        <Route index Component={SecurityList} />
    </Route>
);
