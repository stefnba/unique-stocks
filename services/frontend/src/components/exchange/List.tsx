import { useEffect } from 'react';
import { useAppSelector, useAppDispatch } from '@redux';
import { api as exchangeApi } from '@features/exchange';
import { actions as paginationActions } from '@features/pagination';
import { Card, CardList, PageTitle } from '@sharedComponents';
import ExchangeFilter from './Filter';

export default function ExchangeList() {
    const dispatch = useAppDispatch();
    const pagination = useAppSelector((state) => state.pagination.exchange);
    const filtering = useAppSelector((state) => state.filtering.exchange);

    // api
    const { data, isFetching } = exchangeApi.useExchangeGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
    });
    const { data: count } = exchangeApi.useExchangeGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
    });

    // junmp to page 1 upon new filtering applied
    useEffect(() => {
        dispatch(
            paginationActions.change({
                page: 1,
                pageSize: pagination.pageSize,
                component: 'exchange'
            })
        );
    }, [filtering]);

    return (
        <div>
            <PageTitle title="Exchange" goBack={false} />
            <ExchangeFilter />

            <CardList
                loading={isFetching}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            paginationActions.change({
                                component: 'exchange',
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
                        key={item.id}
                        title={item.name}
                        subTitle={item.mic}
                        link={String(item.id)}
                    />
                )}
            />
        </div>
    );
}
