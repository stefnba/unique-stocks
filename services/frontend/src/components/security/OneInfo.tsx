import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import { api } from '@features/security';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function SecurityOneInfo() {
    const { id: securityId, key } = useParams<{ id: string; key: string }>();

    const { data, error, isLoading } = api.useSecurityGetOneQuery(securityId);

    const navigate = useNavigate();

    const { id, name, isin } = data || {};

    return (
        <>
            <Descriptions title="Security Info" layout="vertical" colon={false}>
                <Descriptions.Item label="ISIN">{isin}</Descriptions.Item>
            </Descriptions>
        </>
    );
}
