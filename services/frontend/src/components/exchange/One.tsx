import type { TabsProps } from 'antd';
import { api } from '@features/exchange';

import { useNavigate, useParams, useLocation } from 'react-router-dom';
import ExchangeOneInfo from './OneInfo';
import OneExchangeSecurity from './security/Security';
import Tabs from 'shared/components/tabs/Tabs';
import PageTitle from '@sharedComponents/title/PageTitle';

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
    }
];

export default function SecurityOne() {
    const { id } = useParams<{ id: string }>();
    const location = useLocation();

    const { data } = api.useExchangeGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate(location?.state?.from || -1);
    };

    const { name, mic } = data || {};

    return (
        <>
            <PageTitle title={`${name} (${mic})`} />
            <Tabs tabs={items} />
        </>
    );
}
