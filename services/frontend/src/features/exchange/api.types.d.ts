export type GetOneExchangeResult = {
    id: number;
    operating_exchange: null | {
        id: number;
        name: string;
        mic: string;
    };
    mic: string;
    name: string;
    country_id: number;
    currency: string;
    website: string;
    timezone: string;
    comment: string;
    acronym: string;
    status: string;
    source: string;
    is_active: boolean;
    is_virtual: boolean;
};

export type ExchangeGetSecurityResult = {
    name: string;
    ticker: string;
    security_listing_id: number;
    security_id: number;
    security_ticker_id: number;
}[];
