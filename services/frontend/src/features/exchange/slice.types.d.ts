export interface LogState {
    security: {
        filtering: {
            applied: { [key: string]: any };
        };
        pagination: {
            page: number;
            pageSize: number;
        };
    };
    filtering: {
        applied: { [key: string]: any };
    };
    pagination: {
        page: number;
        pageSize: number;
    };
}
