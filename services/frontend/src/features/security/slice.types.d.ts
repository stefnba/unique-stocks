export interface LogState {
    filtering: {
        applied: { [key: string]: any };
    };
    pagination: {
        page: number;
        pageSize: number;
    };
}
