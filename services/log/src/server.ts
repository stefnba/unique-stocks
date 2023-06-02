import express, { Application, Request, Response } from 'express';
import bodyParser from 'body-parser';
import { MongoClient } from 'mongodb';
import db from './_db/db';
// import { CONFIG } from './_app';

const url = 'mongodb://localhost:27017/mydb';
// const url = 'mongodb://username:password@localhost:27017/mydb';

const app: Application = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.set('trust proxy', true); // get real ip

app.listen(8000, () => {
    console.log(`Server 1 is running on PORT ${8000}`);
});

app.get('/', async (req: Request, res: Response) => {
    // MongoClient.connect(url).then((db) => {
    //     console.log('Database created!');
    //     db.close();
    // });

    const a = await db.then((d) => d.collect);

    //     (err, db) => {
    //     if (err) throw err;
    //     console.log('Database created!');
    //     db.close();
    // });
    res.send(
        `Hello World!<br/>This app is running on port 8000 and your IP is ${req.ip} <br/> Navigate to /users to list users from the database.`
    );
});

export default app;
