import { Application } from 'express';
import logRouter from './log.js';

const routes = (app: Application) => {
    app.use('/log', logRouter);
};

export default routes;
