import { useEffect } from 'react';

import { useAppSelector, useAppDispatch } from '@redux';

import { api as securityApi } from '@features/security';
import { actions as paginationActions } from '@features/pagination';
import { useParams } from 'react-router-dom';
import { Card, CardList } from '@sharedComponents';

import SecurityFilter from '../Filter';

export default function SecurityListingList() {
    const dispatch = useAppDispatch();

    const { id: securityId } = useParams<{ id: string }>();

    const pagination = useAppSelector(
        (state) => state.pagination.securityListing
    );
    const filtering = useAppSelector(
        (state) => state.filtering.securityListing
    );

    // api
    const { data, isFetching } = securityApi.useSecurityGetListingQuery({
        id: Number.parseInt(securityId),
        filters: {
            page: pagination.page,
            pageSize: pagination.pageSize,
            ...filtering
        }
    });
    const { data: count } = securityApi.useSecurityGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
    });

    const handlePageChange = (page: number, pageSize: number) => {
        dispatch(
            paginationActions.change({
                component: 'security',
                page,
                pageSize
            })
        );
    };

    return (
        <div>
            <CardList
                loading={isFetching}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: handlePageChange,
                    total: count?.count
                }}
                data={data}
                card={(item) => (
                    <Card
                        title={item?.exchange?.name}
                        key={item.id}
                        subTitle={item?.ticker}
                        tags={[item.currency]}
                        link={`/security/listing/${String(item.id)}`}
                    />
                )}
            />
        </div>
    );
}
