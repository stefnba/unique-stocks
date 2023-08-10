import { Descriptions } from 'antd';
import { useParams, Link } from 'react-router-dom';

import { api as entityApi } from '@features/entity';

export default function EntityOneInfo() {
    const { id } = useParams<{ id: string }>();

    const { data: entityData } = entityApi.useEntityGetOneQuery(id);

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
        <Descriptions
            className="mt-6"
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
    );
}
