export type GetAllArgs = {
    pageSize: number;
    page: number;
    [key: string]: unknown;
} | void;
export type GetAllResult = {
    id: number;
    ticker: string;
    name: string;
    isin: string;
}[];
export type GetOneArgs = string;
export type GetOneResult = {
    id: number;
    name: string;
    isin?: string;
};
export type GetCountResult = {
    count: number;
};
