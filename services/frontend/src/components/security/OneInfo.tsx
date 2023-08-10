import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function SecurityOneInfo() {
    const { id: securityId } = useParams<{ id: string }>();

    const { data } = api.useSecurityGetOneQuery(securityId);

    const { id, name, isin } = data || {};

    return (
        <Descriptions className="mt-6" layout="vertical" colon={false}>
            <Descriptions.Item label="ISIN">{isin}</Descriptions.Item>
        </Descriptions>
    );
}
