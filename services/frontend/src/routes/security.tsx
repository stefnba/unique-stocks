import SecurityList from '@components/security/List';
import SecurityOne from '@components/security/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/security">
        <Route index Component={SecurityList} />
        <Route path="listing/:id" Component={SecurityOne} />

        <Route path=":id">
            <Route index Component={SecurityOne} />
            <Route
                path="info"
                Component={SecurityOne}
                handle={{
                    tabKey: 'info'
                }}
            />
            <Route
                path="listing"
                Component={SecurityOne}
                handle={{
                    tabKey: 'listing'
                }}
            />
        </Route>
    </Route>
);
