import { Typography } from 'antd';

import { useAppSelector, useAppDispatch } from '@redux';

import {
    actions as securityActions,
    api as securityApi
} from '@features/security';

import Card from '@sharedComponents/card/Card';
import CardList from '@sharedComponents/cardList/CardList';
import SecurityFilter from './Filter';

const { Title } = Typography;

export default function SecurityList() {
    const dispatch = useAppDispatch();

    const { filtering, pagination } = useAppSelector((state) => state.security);

    // api
    const { data, isLoading } = securityApi.useSecurityGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });
    const { data: count } = securityApi.useSecurityGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });

    return (
        <div>
            <Title>Security</Title>

            <SecurityFilter />

            <CardList
                loading={isLoading}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            securityActions.changePagination({ page, pageSize })
                        );
                    },
                    total: count?.count
                }}
                data={data}
                card={(item) => (
                    <Card
                        title={item.name}
                        subTitle={item.isin}
                        link={String(item.id)}
                    />
                )}
            />
        </div>
    );
}
