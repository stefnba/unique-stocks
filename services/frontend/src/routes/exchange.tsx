import ExchangeList from '@components/exchange/List';
import ExchangeOne from '@components/exchange/One';

import { Route } from 'react-router-dom';

export default (
    <Route path="/exchange">
        <Route index Component={ExchangeList} />
        <Route path=":id">
            <Route index Component={ExchangeOne} />
            <Route
                path="info"
                Component={ExchangeOne}
                handle={{
                    tabKey: 'info'
                }}
            />
            <Route
                path="security"
                Component={ExchangeOne}
                handle={{
                    tabKey: 'security'
                }}
            />
        </Route>
    </Route>
);
