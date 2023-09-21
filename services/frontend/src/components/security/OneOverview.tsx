import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Select, Tabs, Typography } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function SecurityOneOverview() {
    const { id: securityId } = useParams<{ id: string }>();

    const { data } = api.useSecurityGetOneQuery(securityId);

    const { id, name, isin } = data || {};

    return (
        <>
            <Select
                style={{ width: 120 }}
                options={[
                    { value: 'jack', label: 'Jack' },
                    { value: 'lucy', label: 'Lucy' },
                    { value: 'Yiminghe', label: 'yiminghe' },
                    { value: 'disabled', label: 'Disabled', disabled: true }
                ]}
            />
        </>
    );
}
