export interface LogState {
    entity: Filtering;
    entitySecurity: Filtering;
    exchange: Filtering;
    exchangeSecurity: Filtering;
    security: Filtering;
    securityListing: Filtering;
}

export interface Filtering {
    [key: string]: any;
}

export type FilterComponents = keyof LogState;

export type FilteringActionPayload = {
    component: FilterComponents;
    filters: Filtering;
};
