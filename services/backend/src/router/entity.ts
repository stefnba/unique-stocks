import Router from 'express-promise-router';
import * as controller from '@controller/entity.js';

const router = Router();

// public routes
router.get('/', controller.findAll);
router.get('/count', controller.count);
router.get('/filter/choices/:field/', controller.filterChoices);
router.get('/:id/security', controller.findSecurity);
router.get('/:id', controller.findOne);

export default router;
