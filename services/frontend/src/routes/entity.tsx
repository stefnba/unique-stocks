import EntityList from '@components/entity/List';
import EntityOne from '@components/entity/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/entity">
        <Route index Component={EntityList} />
        <Route path=":id/:key" Component={EntityOne} />
        <Route path=":id" Component={EntityOne} />
    </Route>
);
