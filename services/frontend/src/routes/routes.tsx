import { Route } from 'react-router-dom';

import App from '@components/app/App';
import HomeRoutes from './home/routes';
import UserRoutes from './user/routes';
import LogRoutes from './log/routes';
import ExchangeRoutes from './exchange';
import EntityRoutes from './entity';
import IndexRoutes from './index';
import SecurityRoutes from './security';
import WatchlistRoutes from './watchlist';

export default (
    <Route Component={App}>
        {HomeRoutes}
        {UserRoutes}
        {LogRoutes}
        {ExchangeRoutes}
        {EntityRoutes}
        {IndexRoutes}
        {SecurityRoutes}
        {WatchlistRoutes}
    </Route>
);
