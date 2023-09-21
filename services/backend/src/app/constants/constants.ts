/**
 * Http status types
 */
export enum httpStatus {
    BAD_REQUEST_400 = 400,
    CREATED_201 = 201,
    SUCCESS_200 = 200,
    SERVER_ERROR_500 = 500,
    UNAUTHORIZED_401 = 401,
    FORBIDDEN_403 = 403,
    NOT_FOUND_404 = 404
}
/**
 * Maps http method to a http status code, useful for generic controller response
 */
export enum httpMethodStatusMapping {
    GET = httpStatus.SUCCESS_200,
    POST = httpStatus.CREATED_201,
    PUT = httpStatus.CREATED_201,
    DELETE = httpStatus.CREATED_201
}
/**
 * Error types
 */
export enum Errors {
    Conflict = 'CONFLICT',
    Connection = 'CONNECTION',
    Query = 'QUERY',
    QueryFile = 'QUERY_FILE',
    Database = 'DATABASE',
    NotFound = 'NOT_FOUND',
    Validation = 'VALIDATION',
    Service = 'SERVICE',
    Exception = 'EXCEPTION',
    General = 'GENERAL',
    Response = 'RESPONSE',
    Auth = 'AUTHORIZATION',
    Info = 'INFO',
    Other = 'OTHER'
}
/**
 * Logger types
 */
export enum Loggers {
    Application = 'APPLICATION',
    Server = 'SERVER',
    Service = 'SERVICE',
    Database = 'DATABASE',
    Middleware = 'MIDDLEWARE',
    Http = 'HTTP',
    Socket = 'SOCKET',
    Query = 'QUERY', // only used for logging db query. Other db related logging, Database logger to be used
    Mail = 'MAIL',
    Process = 'PROCESS'
}
