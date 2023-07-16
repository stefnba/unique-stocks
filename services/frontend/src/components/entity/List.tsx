import * as entityApi from '@features/entity/api';
import { useAppSelector, useAppDispatch } from '@redux';
import { List, Typography, Card } from 'antd';

import { Link } from 'react-router-dom';

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
                        <Link to={`${entity.id}`}>
                            <Card title={entity.name} />
                        </Link>
                    </List.Item>
                )}
                size="small"
                grid={{
                    gutter: 16,
                    xs: 1,
                    sm: 2,
                    md: 4,
                    lg: 4,
                    xl: 6,
                    xxl: 4
                }}
            />
        </>
    );
}
