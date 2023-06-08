import Router from 'express-promise-router';
import controller from './controller.js';

const router = Router();

// public routes
router.get('/', controller.findAll);
router.get('/:id', controller.findOne);

export default router;
