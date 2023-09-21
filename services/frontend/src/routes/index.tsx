import IndexList from '@components/index/List';

import { Route } from 'react-router-dom';

export default (
    <Route path="/index">
        <Route index Component={IndexList} />
    </Route>
);
