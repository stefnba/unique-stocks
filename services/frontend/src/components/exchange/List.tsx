import { Typography } from 'antd';

import { useAppSelector, useAppDispatch } from '@redux';

import {
    api as exchangeApi,
    actions as exchangeActions
} from '@features/exchange';

import ExchangeFilter from './Filter';
import Card from '@sharedComponents/card/Card';
import CardList from '@sharedComponents/cardList/CardList';
import { useEffect } from 'react';

const { Title } = Typography;

export default function ExchangeList() {
    const dispatch = useAppDispatch();
    const { filtering, pagination } = useAppSelector((state) => state.exchange);

    // api
    const { data, isLoading } = exchangeApi.useExchangeGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });
    const { data: count } = exchangeApi.useExchangeGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });

    // junmp to page 1 upon new filtering applied
    useEffect(() => {
        dispatch(
            exchangeActions.changePagination({
                page: 1,
                pageSize: pagination.pageSize
            })
        );
    }, [filtering.applied]);

    return (
        <div>
            <Title>Exchange</Title>
            <ExchangeFilter />

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
                        subTitle={item.mic}
                        link={String(item.id)}
                    />
                )}
            />
        </div>
    );
}
