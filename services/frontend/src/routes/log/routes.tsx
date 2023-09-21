import LogList from '@components/log/List';
import LogOne from '@components/log/One';

import { Route } from 'react-router-dom';

export default (
    <Route>
        <Route path="/log" Component={LogList} />
        <Route path="/log/:id" Component={LogOne} />
    </Route>
);
// export default <Route path="/log" Component={LogList} />;
