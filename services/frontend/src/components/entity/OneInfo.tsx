import { Button, Descriptions, Tabs, Typography } from 'antd';

import { api as entityApi } from '@features/entity';

import { useNavigate, useParams, Link } from 'react-router-dom';

export default function EntityOneInfo() {
    const { id, key } = useParams<{ id: string; key: string }>();

    const { data: entityData } = entityApi.useEntityGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate('/entity');
    };

    const {
        name,
        lei,
        website,
        legal_address_city,
        legal_address_country,
        legal_address_street_number,
        legal_address_zip_code,
        legal_address_street,
        headquarter_address_city,
        headquarter_address_country,
        headquarter_address_street,
        headquarter_address_street_number,
        headquarter_address_zip_code,
        description
    } = entityData || {};

    return (
        <>
            <Descriptions
                title="Entity Info"
                layout="vertical"
                colon={false}
                column={4}
            >
                <Descriptions.Item label="LEI">{lei}</Descriptions.Item>
                <Descriptions.Item label="Legal Address">
                    {legal_address_street}
                    <br />
                    {legal_address_zip_code} {legal_address_city}, &nbsp;
                    {legal_address_country}
                </Descriptions.Item>
                <Descriptions.Item label="Headquarter Address">
                    {headquarter_address_street}
                    <br />
                    {headquarter_address_zip_code} {headquarter_address_city},
                    &nbsp;
                    {headquarter_address_country}
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
