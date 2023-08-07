import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions, Tabs, Typography } from 'antd';

import { api } from '@features/exchange';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function ExchangeOneInfo() {
    const { id, key } = useParams<{ id: string; key: string }>();

    const { data, error, isLoading } = api.useExchangeGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/exchange');
    };

    const {
        mic,
        comment,
        currency,
        website,
        operating_exchange,
        acronym,
        country_id,
        is_active,
        is_virtual,
        source,
        status,
        timezone
    } = data || {};

    return (
        <>
            <Descriptions title="Exchange Info" layout="vertical" colon={false}>
                <Descriptions.Item label="MIC">{mic}</Descriptions.Item>
                {operating_exchange && (
                    <Descriptions.Item label="Operating Exchange">
                        <Link to={`/exchange/${String(operating_exchange.id)}`}>
                            {operating_exchange.name}
                        </Link>
                    </Descriptions.Item>
                )}

                <Descriptions.Item label="Currency">
                    {currency}
                </Descriptions.Item>
                <Descriptions.Item label="Country">
                    {country_id}
                </Descriptions.Item>
                <Descriptions.Item label="Country">
                    {is_active}
                </Descriptions.Item>
                <Descriptions.Item label="Virtual">
                    {is_virtual}
                </Descriptions.Item>
                <Descriptions.Item label="Status">{status}</Descriptions.Item>
                <Descriptions.Item label="Source">{source}</Descriptions.Item>
                <Descriptions.Item label="Acronym">{acronym}</Descriptions.Item>
                <Descriptions.Item label="Comment">{comment}</Descriptions.Item>
                <Descriptions.Item label="Website">
                    <Link to={`http://${website}`} target="_blank">
                        {website}
                    </Link>
                </Descriptions.Item>
            </Descriptions>
        </>
    );
}
