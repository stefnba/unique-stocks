import express, { Application } from 'express';
import bodyParser from 'body-parser';

import cors from 'cors';
import multer from 'multer';
import http from 'http';
import cookieParser from 'cookie-parser';
import helmet from 'helmet';

import config from '@config';
import router from './router.js';

import {
    // morganMiddleware,
    ignoreFaviconMiddleware,
    correlationId,
    userAgent
} from '@middleware';

// App setup =============================
const upload = multer();
const app: Application = express();
app.set('trust proxy', true); // get real ip

const server = http.createServer(app);

// Middlewares =============================
app.use(correlationId);
// app.use(userAgent);
app.use(ignoreFaviconMiddleware);
app.use(helmet());
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(upload.any());
// app.use(morganMiddleware);
app.use(cors(config.cors));
app.use(cookieParser());

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Static files =============================
app.use('/media', express.static(config.app.mediaDir));

// Routes =============================
router(app);

server.listen(config.app.port, () => {
    console.log(`Server is running on PORT ${config.app.port}`);
});

export default app;
