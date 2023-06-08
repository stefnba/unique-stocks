import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';
import collections from '@root/_db/collections.js';
import config from '@root/_app/config.js';

const app: Application = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.set('trust proxy', true); // get real ip

app.listen(config.app.port, () => {
    console.log(`Server is running on PORT ${config.app.port}`);
});

app.get('/log', async (req: Request, res: Response) => {
    const users = await collections.log.findMany();
    res.json(users);
});

app.post('/log/add', async (req: Request, res: Response) => {
    const users = await collections.user.addOne(req.body);
    res.json(users);
});

app.get('/', async (req: Request, res: Response) => {
    res.send(
        'Hello World!<br/> This is the centralized logging service for Unique Stocks'
    );
});

export default app;
