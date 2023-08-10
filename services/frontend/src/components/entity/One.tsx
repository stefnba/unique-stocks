import type { TabsProps } from 'antd';
import { api as entityApi } from '@features/entity';

import { useNavigate, useParams, useLocation } from 'react-router-dom';
import ExchangeOneInfo from './OneInfo';
import PageTitle from '@sharedComponents/title/PageTitle';
import Tabs from '@sharedComponents/tabs/Tabs';

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
    const { id } = useParams<{ id: string }>();

    const { data: entityData } = entityApi.useEntityGetOneQuery(id);

    const { name } = entityData || {};

    return (
        <div>
            <PageTitle title={name} />
            <Tabs basePath={['entity', id]} tabs={items} />
        </div>
    );
}
