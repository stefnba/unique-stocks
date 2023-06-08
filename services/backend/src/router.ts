import { Application } from 'express';

import exchangeRouter from './exchange/router.js';
import entityRouter from './entity/router.js';

const routes = (app: Application) => {
    app.use('/exchange', exchangeRouter);
    app.use('/entity', entityRouter);
};

export default routes;
