import { Application } from 'express';

import exchangeRouter from './exchange/router';
import entityRouter from './entity/router';

const routes = (app: Application) => {
    app.use('/exchange', exchangeRouter);
    app.use('/entity', entityRouter);
};

export default routes;
