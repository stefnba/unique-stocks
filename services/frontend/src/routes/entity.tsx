import EntityList from '@components/entity/List';
import EntityOne from '@components/entity/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/entity">
        <Route index Component={EntityList} />
        <Route path=":id">
            <Route index Component={EntityOne} />
            <Route
                path="info"
                Component={EntityOne}
                handle={{
                    tabKey: 'info'
                }}
            />
            <Route
                path="fundamental"
                Component={EntityOne}
                handle={{
                    tabKey: 'fundamental'
                }}
            />
            <Route
                path="entity"
                Component={EntityOne}
                handle={{
                    tabKey: 'entity'
                }}
            />
            <Route
                path="security"
                Component={EntityOne}
                handle={{
                    tabKey: 'security'
                }}
            />
            <Route
                path="overview"
                Component={EntityOne}
                handle={{
                    tabKey: 'overview'
                }}
            />
        </Route>
    </Route>
);
