import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import type { TabsProps } from 'antd';
import { api } from '@features/exchange';

import { useNavigate, useParams, useLocation } from 'react-router-dom';
import ExchangeOneInfo from './OneInfo';
import OneExchangeSecurity from './Security';

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
        label: 'Security',
        children: <OneExchangeSecurity />
    },
    {
        key: 'index',
        label: `Index`,
        children: `To come...`
    }
];

export default function SecurityOne() {
    const { id, key } = useParams<{ id: string; key: string }>();
    const location = useLocation();

    const { data, error, isLoading } = api.useExchangeGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/exchange');
    };

    const { name, mic } = data || {};

    return (
        <>
            <Title>
                {name} ({mic})
            </Title>
            <Button onClick={goBack}>Back</Button>

            <Tabs
                onTabClick={(key) => navigate(`/exchange/${id}/${key}`)}
                activeKey={key}
                defaultActiveKey="info"
                items={items}
                onChange={onChange}
            />
        </>
    );
}
