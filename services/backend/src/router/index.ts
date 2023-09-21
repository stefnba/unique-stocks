import { Application } from 'express';

import exchangeRouter from '@routes/exchange.js';
import entityRouter from '@routes/entity.js';
import logRouter from '@routes/log.js';
import securityRouter from '@routes/security.js';

const routes = (app: Application) => {
    app.use('/exchange', exchangeRouter);
    app.use('/entity', entityRouter);
    app.use('/log', logRouter);
    app.use('/security', securityRouter);
};

export default routes;
