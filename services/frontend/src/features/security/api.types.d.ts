export type GetOneSecurityResult = {
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

type GetAllExchangeResult = {
    id: number;
    ticker: string;
    name: string;
};
