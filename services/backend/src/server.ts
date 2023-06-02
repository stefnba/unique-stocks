import express, { Application } from 'express';
import bodyParser from 'body-parser';

import path from 'path';
import cors from 'cors';
import multer from 'multer';
import http from 'http';
import cookieParser from 'cookie-parser';
import helmet from 'helmet';

import { config } from './_app';
import router from './router';

import {
    morganMiddleware,
    ignoreFaviconMiddleware,
    correlationId,
    userAgent
} from './_middleware';

// App setup =============================
const upload = multer();
const app: Application = express();
app.set('trust proxy', true); // get real ip

const server = http.createServer(app);

// Middlewares =============================
app.use(correlationId);
app.use(userAgent);
app.use(ignoreFaviconMiddleware);
app.use(helmet());
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(upload.any());
app.use(morganMiddleware);
app.use(cors(config.cors));
app.use(cookieParser());

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Static files =============================
app.use('/media', express.static(path.join(__dirname, '../../media')));

// Routes =============================
router(app);

// app.get('/users', async (req: Request, res: Response) => {
//     try {
//         const users = await db.app.query.find('SELECT * FROM users').many();
//         res.json(users);
//     } catch (err) {
//         res.status(500).send(err);
//     }
// });

// app.get('/exchanges', async (req: Request, res: Response) => {
//     try {
//         const users = await db.stocks.query
//             .find('SELECT * FROM exchange LIMIT 10')
//             .many();
//         res.json(users);
//     } catch (err) {
//         res.status(500).send(err);
//     }
// });

// app.get('/', (req: Request, res: Response) => {
//     res.send(
//         `Hello World!<br/>This app is running on port ${config.app.port} and your IP is ${req.ip} <br/> Navigate to /users to list users from the database.`
//     );
// });

server.listen(config.app.port, () => {
    console.log(`Server is running on PORT ${config.app.port}`);
});

export default app;
