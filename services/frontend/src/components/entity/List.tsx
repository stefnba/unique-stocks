import { List, Typography } from 'antd';
import { Link } from 'react-router-dom';

import { useAppSelector, useAppDispatch } from '@redux';
import { api as entityApi, actions as entityActions } from '@features/entity';
import Card from '@sharedComponents/card/Card';
import CardList from '@sharedComponents/cardList/CardList';
import EntityFilter from './Filter';

const { Title } = Typography;

export default function EntityList() {
    const dispatch = useAppDispatch();
    const { filtering, pagination } = useAppSelector((state) => state.entity);

    const { data: entityData, isLoading } = entityApi.useEntityGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });

    const { data: count } = entityApi.useEntityGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering.applied
    });

    return (
        <>
            <Title>Entity Overview</Title>
            <EntityFilter />

            <CardList
                loading={isLoading}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            entityActions.changePagination({ page, pageSize })
                        );
                    },
                    total: count?.count
                }}
                data={entityData}
                card={(item) => (
                    <Card
                        subTitle="sector"
                        title={item.name}
                        link={String(item.id)}
                        tags={[item.legal_address_country]}
                    />
                )}
            />
        </>
    );
}
