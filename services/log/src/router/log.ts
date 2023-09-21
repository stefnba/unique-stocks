import Router from 'express-promise-router';

import * as logController from '../controller/log.js';

const router = Router();

router.get('/', logController.findAll);
router.post('/add', logController.addOne);
router.get('/distinct', logController.getDistinctValues);
router.get('/:id', logController.findOne);

export default router;
