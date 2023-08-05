import controllerHandler from '@lib/controller/handler.js';
import * as securityService from '@service/security.js';

type FindOneRequestArgs = {
    params: { id: number };
};

export const findAll = controllerHandler(() => securityService.findAll());

export const findOne = controllerHandler<FindOneRequestArgs>(({ params }) =>
    securityService.findOne(params.id)
);
