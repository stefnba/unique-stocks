import type { TabsProps } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams } from 'react-router-dom';
import SecurityOneInfo from './OneInfo';
import SecurityListing from './listing/Listing';
import PageTitle from '@sharedComponents/title/PageTitle';
import Tabs from '@sharedComponents/tabs/Tabs';

const items: TabsProps['items'] = [
    {
        key: 'info',
        label: 'Info',
        children: <SecurityOneInfo />
    },
    {
        key: 'listing',
        label: `Listing`,
        children: <SecurityListing />
    }
];

export default function ExchangeOne() {
    const { id, key } = useParams<{ id: string; key: string }>();
    const navigate = useNavigate();

    const { data } = api.useSecurityGetOneQuery(id);

    const { name } = data || {};

    return (
        <>
            <PageTitle title={name} />
            <Tabs tabs={items} />
        </>
    );
}
