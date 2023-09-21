import { useAppSelector, useAppDispatch } from '@redux';

import { api as securityApi } from '@features/security';
import { actions as paginationActions } from '@features/pagination';

import { Card, CardList, PageTitle } from '@sharedComponents';
import SecurityFilter from './Filter';

export default function SecurityList() {
    const dispatch = useAppDispatch();

    const pagination = useAppSelector((state) => state.pagination.security);
    const filtering = useAppSelector((state) => state.filtering.security);

    // api
    const { data, isFetching } = securityApi.useSecurityGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
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
            <PageTitle goBack={false} title="Security" />

            <SecurityFilter />

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
                        key={item.id}
                        title={item.name}
                        subTitle={item.isin}
                        link={String(item.id)}
                    />
                )}
            />
        </div>
    );
}
