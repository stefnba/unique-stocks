import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { docco } from 'react-syntax-highlighter/dist/esm/styles/hljs';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import type { TabsProps } from 'antd';
import { useGetOneEntityQuery } from '@features/entity/api';

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
        key: 'security',
        label: `Security`,
        children: `To come...`
    },
    {
        key: 'entity',
        label: `Related Entity`,
        children: `To come...`
    }
];

export default function EntityOne() {
    const { id, key } = useParams<{ id: string; key: string }>();
    const location = useLocation();

    const { data: entityData } = useGetOneEntityQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/entity');
    };

    const { name } = entityData || {};

    console.log(location);

    return (
        <div>
            <Title>{name}</Title>
            <Button onClick={goBack}>Back</Button>

            <Tabs
                onTabClick={(key) => navigate(`/entity/${id}/${key}`)}
                activeKey={key}
                defaultActiveKey="info"
                items={items}
                onChange={onChange}
            />
        </div>
    );
}
