import { Route } from 'react-router-dom';
import App from '@components/app/App';
import HomeRoutes from './home/routes';
import UserRoutes from './user/routes';

export default (
    <Route Component={App}>
        {HomeRoutes}
        {UserRoutes}
    </Route>
);
