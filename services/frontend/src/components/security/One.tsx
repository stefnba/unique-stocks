import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import type { TabsProps } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams, useLocation } from 'react-router-dom';
import SecurityOneInfo from './OneInfo';

const { Title } = Typography;

const items: TabsProps['items'] = [
    {
        key: 'info',
        label: 'Info',
        children: <SecurityOneInfo />
    },
    {
        key: 'listing',
        label: `Listing`,
        children: `To come...`
    }
];

export default function ExchangeOne() {
    const { id, key } = useParams<{ id: string; key: string }>();
    const location = useLocation();

    const { data, error, isLoading } = api.useSecurityGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/security');
    };

    const { name } = data || {};

    return (
        <>
            <Title>{name}</Title>
            <Button onClick={goBack}>Back</Button>

            <Tabs
                onTabClick={(key) => navigate(`/security/${id}/${key}`)}
                activeKey={key}
                defaultActiveKey="info"
                items={items}
                // onChange={onChange}
            />
        </>
    );
}
