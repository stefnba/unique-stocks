import SecurityList from '@components/security/List';
import SecurityOne from '@components/security/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/security">
        <Route index Component={SecurityList} />
        <Route path="listing/:id" Component={SecurityOne} />
        <Route path=":id/:key" Component={SecurityOne} />
        <Route path=":id" Component={SecurityOne} />
    </Route>
);
