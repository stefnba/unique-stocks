import SecurityList from '@components/security/List';
import SecurityOne from '@components/security/One';
import SecurityListingOne from '@components/security/listing/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/security">
        <Route index Component={SecurityList} />
        <Route path="listing/:id" Component={SecurityListingOne} />
        <Route path=":id">
            <Route index Component={SecurityOne} />
            <Route
                path="overview"
                Component={SecurityOne}
                handle={{
                    tabKey: 'overview'
                }}
            />

            <Route
                path="info"
                Component={SecurityOne}
                handle={{
                    tabKey: 'info'
                }}
            />
            <Route
                path="chart"
                Component={SecurityOne}
                handle={{
                    tabKey: 'chart'
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
