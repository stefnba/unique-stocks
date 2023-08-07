import { useParams } from 'react-router-dom';
import { useAppSelector, useAppDispatch } from '@redux';

import {
    api as exchangeApi,
    actions as exchangeActions
} from '@features/exchange';
import ExchangeFilter from './Filter';
import Card from '@sharedComponents/card/Card';
import CardList from '@sharedComponents/cardList/CardList';

type ParamProps = {
    id: string;
};

export default function OneExchangeSecurity() {
    const dispatch = useAppDispatch();

    const { id } = useParams<ParamProps>();

    const { filtering, pagination } = useAppSelector((state) => state.exchange);

    // api
    const { data, isLoading } = exchangeApi.useExchangeGetSecurityQuery(id);
    const { data: count } = exchangeApi.useExchangeGetSecurityCountQuery(id);
    return (
        <>
            {/* <ExchangeFilter /> */}
            <p>Overview of all listed Securities:</p>

            <CardList
                loading={isLoading}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            exchangeActions.changePagination({ page, pageSize })
                        );
                    },
                    total: count?.count
                }}
                data={data}
                card={(item) => (
                    <Card
                        title={item.name}
                        subTitle={item.ticker}
                        link={String(item.security_listing_id)}
                    />
                )}
            />
        </>
    );
}
