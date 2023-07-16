import ExchangeList from '@components/exchange/List';
import ExchangeOne from '@components/exchange/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/exchange">
        <Route index Component={ExchangeList} />
        <Route path=":id/:key" Component={ExchangeOne} />
        <Route path=":id" Component={ExchangeOne} />
    </Route>
);
