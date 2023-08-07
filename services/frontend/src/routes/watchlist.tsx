import WatchlistList from '@components/watchlist/List';

import { Route } from 'react-router-dom';

export default (
    <Route path="/watchlist">
        <Route index Component={WatchlistList} />
    </Route>
);
