type Pagination = {
    page: number;
    pageSize: number;
};

export interface LogState {
    entity: Pagination;
    entitySecurity: Pagination;
    exchange: Pagination;
    exchangeSecurity: Pagination;
    security: Pagination;
    securityListing: Pagination;
}

export type PaginationComponents = keyof LogState;
