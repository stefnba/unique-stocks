import { useParams } from 'react-router-dom';
import { useAppSelector, useAppDispatch } from '@redux';

import { api as exchangeApi } from '@features/exchange';
import { actions as paginationActions } from '@features/pagination';

import Card from '@sharedComponents/card/Card';
import CardList from '@sharedComponents/cardList/CardList';
import ExchangeSecurityFilter from './SecurityFilter';

type ParamProps = {
    id: string;
};

export default function OneExchangeSecurity() {
    const dispatch = useAppDispatch();

    const { id } = useParams<ParamProps>();

    const filtering = useAppSelector(
        (state) => state.filtering.exchangeSecurity
    );
    const pagination = useAppSelector(
        (state) => state.pagination.exchangeSecurity
    );

    // api
    const { data, isLoading } = exchangeApi.useExchangeGetSecurityQuery(id);
    const { data: count } = exchangeApi.useExchangeGetSecurityCountQuery(id);
    return (
        <>
            <p>Overview of all listed Securities:</p>
            <ExchangeSecurityFilter />

            <CardList
                loading={isLoading}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            paginationActions.change({
                                component: 'exchangeSecurity',
                                page,
                                pageSize
                            })
                        );
                    },
                    total: count?.count
                }}
                data={data}
                card={(item) => (
                    <Card
                        key={item.security_listing_id}
                        title={item.name}
                        subTitle={item.ticker}
                        link={`/listing/${String(item.security_listing_id)}`}
                    />
                )}
            />
        </>
    );
}
