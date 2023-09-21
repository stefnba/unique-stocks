export type GetAllArgs = {
    pageSize: number;
    page: number;
    [key: string]: unknown;
} | void;

export type GetListingArgs = {
    id: number;
    filters: GetAllArgs;
};

export type GetListingResult = {
    id: number;
    figi: string;
    ticker: string;
    currency: string;
    quote_source: string;
    exchange: {
        id: number;
        name: string;
        mic: string;
    };
}[];

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

export type GetOneListingResult = {
    id: number;
    figi: string;
    ticker: string;
    currency: string;
    quote_source: string;
    exchange: {
        id: number;
        name: string;
        mic: string;
    };
    quotes: { close: number; date: string }[];
};
