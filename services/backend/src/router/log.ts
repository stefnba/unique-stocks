import Router from 'express-promise-router';
import * as controller from '@controller/log.js';

const router = Router();

// public routes
router.get('/', controller.findAll);
router.get('/:field/choices', controller.getFieldChoices);
router.get('/:id', controller.findOne);

export default router;
