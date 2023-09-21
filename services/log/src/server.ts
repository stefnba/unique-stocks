import express, { Application } from 'express';
import bodyParser from 'body-parser';

import config from '@root/app/config/config.js';
import router from '@router';

const app: Application = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.set('trust proxy', true); // get real ip

// Routes =============================
router(app);

// Launch =============================
app.listen(config.app.port, () => {
    console.log(`Server is running on PORT ${config.app.port}`);
});

export default app;
