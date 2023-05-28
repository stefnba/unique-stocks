import chai from 'chai';
import mocha from 'mocha';
import supertest from 'supertest';
import app from '../src/server';

const request = supertest(app);

const { it, describe } = mocha;
const { expect } = chai;

describe('TESTS', () => {
    describe('HOME', () => {
        it('SHOULD PRINT Hello World', async () => {
            const res = await request.get('/');
            expect(res.status).to.eql(200);
        });
    });
    describe('USERS', () => {
        it('SHOULD LIST users', async () => {
            const res = await request.get('/users');
            expect(res.status).to.eql(200);
            expect(res.body).to.be.an('array');
            expect(res.body[0]).to.have.keys(['id', 'username', 'email']);
        });
    });
});
