import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';

import db from './_db';
import { CONFIG } from './_app';

const app: Application = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.set('trust proxy', true); // get real ip

app.get('/users', async (req: Request, res: Response) => {
    try {
        const users = await db.app.query.find('SELECT * FROM users').many();
        res.json(users);
    } catch (err) {
        res.status(500).send(err);
    }
});

app.get('/exchanges', async (req: Request, res: Response) => {
    try {
        const users = await db.stocks.query
            .find('SELECT * FROM exchange LIMIT 10')
            .many();
        res.json(users);
    } catch (err) {
        res.status(500).send(err);
    }
});

app.get('/', (req: Request, res: Response) => {
    res.send(
        `Hello World!<br/>This app is running on port ${CONFIG.app.port} and your IP is ${req.ip} <br/> Navigate to /users to list users from the database.`
    );
});

app.listen(CONFIG.app.port, () => {
    console.log(`Server is running on PORT ${CONFIG.app.port}`);
});

export default app;
