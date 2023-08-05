import { List, Typography } from 'antd';
import { Link } from 'react-router-dom';

import { useAppSelector, useAppDispatch } from '@redux';
import * as entityApi from '@features/entity/api';
import Card from '@sharedComponents/card/Card';

const { Title } = Typography;

export default function EntityList() {
    const { data: entityData } = entityApi.useGetAllEntityQuery();

    return (
        <>
            <Title>Entity Overview</Title>
            <List
                pagination={{
                    onChange: (page) => {
                        console.log(page);
                    },
                    pageSize: 20
                }}
                dataSource={entityData}
                renderItem={(entity) => (
                    <List.Item key={entity.id}>
                        <Card
                            subTitle="sector"
                            title={entity.name}
                            link={String(entity.id)}
                            tags={[entity.legal_address_country]}
                        />
                    </List.Item>
                )}
                grid={{
                    gutter: 18,
                    xs: 1,
                    sm: 2,
                    md: 2,
                    lg: 3,
                    xl: 4,
                    xxl: 4
                }}
            />
        </>
    );
}
