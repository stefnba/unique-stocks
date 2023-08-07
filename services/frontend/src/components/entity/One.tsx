import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import type { TabsProps } from 'antd';
import { api as entityApi } from '@features/entity';

import { useNavigate, useParams, useLocation } from 'react-router-dom';
import ExchangeOneInfo from './OneInfo';

const { Title } = Typography;

const onChange = (key: string) => {
    console.log(key);
};

const items: TabsProps['items'] = [
    {
        key: 'info',
        label: 'Info',
        children: <ExchangeOneInfo />
    },
    {
        key: 'overview',
        label: `Overview`,
        children: `To come...`
    },
    {
        key: 'security',
        label: `Security`,
        children: `To come...`
    },
    {
        key: 'entity',
        label: `Related Entity`,
        children: `To come...`
    },
    {
        key: 'fundamental',
        label: `Fundamental`,
        children: `To come...`
    }
];

export default function EntityOne() {
    const { id, key } = useParams<{ id: string; key: string }>();
    const location = useLocation();

    const { data: entityData } = entityApi.useEntityGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/entity');
    };

    const { name } = entityData || {};

    return (
        <div>
            <Title>{name}</Title>
            <Button onClick={goBack}>Back</Button>

            <Tabs
                className="mt-8"
                onTabClick={(key) => navigate(`/entity/${id}/${key}`)}
                activeKey={key}
                defaultActiveKey="info"
                items={items}
                onChange={onChange}
            />
        </div>
    );
}
