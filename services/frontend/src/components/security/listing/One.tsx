import { PageTitle } from '@sharedComponents/index';
import { useAppSelector, useAppDispatch } from '@redux';
import { useParams } from 'react-router-dom';

import { api as securityApi } from '@features/security';
import { Table } from 'antd';

export default function SecurityListingOne() {
    const dispatch = useAppDispatch();

    const { id: securityListingId } = useParams<{ id: string }>();
    const { data } =
        securityApi.useSecurityGetOneListingQuery(securityListingId);
    return (
        <>
            <PageTitle title={data?.figi} />

            <Table
                size="small"
                dataSource={data?.quotes}
                columns={[
                    {
                        dataIndex: 'close',
                        key: 'close',
                        title: 'Close'
                    },
                    {
                        dataIndex: 'date',
                        key: 'date',
                        title: 'Date'
                    }
                ]}
            />

            {/* {data?.quotes.map((q) => q.close)} */}
        </>
    );
}
