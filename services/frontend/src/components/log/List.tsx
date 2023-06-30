import { useAppSelector, useAppDispatch } from '@redux';

import { Table } from 'antd';

import * as logApi from '@features/log/api';

import { Link } from 'react-router-dom';
import LogFilter from './Filter';

export default function LogList() {
    const { applied } = useAppSelector((state) => state.log.filtering);
    const dispatch = useAppDispatch();
    const { data, error, isLoading } = logApi.useGetAllQuery(applied);

    return (
        <div>
            <LogFilter />
            <Table
                dataSource={data}
                pagination={{
                    defaultPageSize: 50
                }}
                columns={[
                    {
                        title: 'Timestamp',
                        dataIndex: 'loggedAt',
                        render: (text: string, record) => (
                            <Link to={record._id}>{text}</Link>
                        )
                    },
                    {
                        title: 'Level',
                        dataIndex: 'levelname',
                        key: 'levelname',
                        filters: [
                            {
                                text: 'Joe',
                                value: 'Joe'
                            }
                        ]
                    },
                    {
                        title: 'Logger',
                        dataIndex: 'name',
                        key: 'name'
                    },
                    {
                        title: 'Event',
                        dataIndex: 'event',
                        key: 'event'
                    },
                    {
                        title: 'Message',
                        dataIndex: 'msg',
                        key: 'msg'
                    },
                    {
                        title: 'Service',
                        dataIndex: 'service',
                        key: 'service'
                    }
                ]}
            />
        </div>
    );
}
