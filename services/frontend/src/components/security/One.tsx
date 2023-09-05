import type { TabsProps } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams } from 'react-router-dom';
import SecurityOneInfo from './OneInfo';
import SecurityOneOverview from './OneOverview';
import SecurityListingList from './listing/List';
import PageTitle from '@sharedComponents/title/PageTitle';
import Tabs from '@sharedComponents/tabs/Tabs';

const items: TabsProps['items'] = [
    {
        key: 'overview',
        label: 'Overview',
        children: <SecurityOneOverview />
    },
    {
        key: 'info',
        label: 'Info',
        children: <SecurityOneInfo />
    },
    {
        key: 'chart',
        label: 'Chart',
        children: <SecurityOneInfo />
    },
    {
        key: 'listing',
        label: `Listing`,
        children: <SecurityListingList />
    }
];

export default function SecurityOne() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();

    const { data } = api.useSecurityGetOneQuery(id);

    const { name } = data || {};

    return (
        <>
            <PageTitle title={name} />
            <Tabs basePath={['security', id]} tabs={items} />
        </>
    );
}
