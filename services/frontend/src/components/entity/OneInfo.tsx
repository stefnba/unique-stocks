import { Button, Descriptions, Tabs, Typography } from 'antd';

import { useGetOneEntityQuery } from '@features/entity/api';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function EntityOneInfo() {
    const { id, key } = useParams<{ id: string; key: string }>();

    const { data: entityData } = useGetOneEntityQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/exchange');
    };

    const {
        name,
        lei,
        website,
        legal_address_city,
        legal_address_country,
        description
    } = entityData || {};

    return (
        <>
            <Descriptions title="Entity Info" layout="vertical" colon={false}>
                <Descriptions.Item label="LEI">{lei}</Descriptions.Item>
                <Descriptions.Item label="Description">
                    {description}
                </Descriptions.Item>

                <Descriptions.Item label="Website">
                    <Link to={`http://${website}`} target="_blank">
                        {website}
                    </Link>
                </Descriptions.Item>
            </Descriptions>
        </>
    );
}
