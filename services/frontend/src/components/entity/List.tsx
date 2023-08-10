import { useAppSelector, useAppDispatch } from '@redux';
import { api as entityApi } from '@features/entity';
import { actions as paginationActions } from '@features/pagination';

import { Card, PageTitle, CardList } from '@sharedComponents';
import EntityFilter from './Filter';

export default function EntityList() {
    const dispatch = useAppDispatch();
    const pagination = useAppSelector((state) => state.pagination.entity);
    const filtering = useAppSelector((state) => state.filtering.entity);

    const { data: entityData, isFetching } = entityApi.useEntityGetAllQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
    });

    const { data: count } = entityApi.useEntityGetCountQuery({
        page: pagination.page,
        pageSize: pagination.pageSize,
        ...filtering
    });

    return (
        <>
            <PageTitle title="Exchange" goBack={false} />
            <EntityFilter />

            <CardList
                loading={isFetching}
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    onChange: (page: number, pageSize: number) => {
                        dispatch(
                            paginationActions.change({
                                component: 'entity',
                                page,
                                pageSize
                            })
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
