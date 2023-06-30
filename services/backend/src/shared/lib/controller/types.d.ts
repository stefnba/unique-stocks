export interface ControllerOptions {
    statusCode?: number;
    responseMsg?: string;
    hideResponseMsg?: boolean;
}

export type RequestArgsBase = {
    body?: unknown;
    query?: unknown;
    params?: unknown;
};

type ControllerHandlerFunc<
    RequestArgs extends RequestArgsBase = RequestArgsBase
> = (
    user?: string,
    requestArgs?: Pick<RequestArgs, 'body' | 'query' | 'params'>
) => Promise<unknown>;
