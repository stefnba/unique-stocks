import { Table } from 'antd';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';

import { useAppSelector, useAppDispatch } from '@redux';

import * as logApi from '@features/log/api';
import { actions } from '@features/log/slice';

import { Link } from 'react-router-dom';
import LogFilter from './Filter';

dayjs.extend(utc);
dayjs.extend(timezone);

export default function LogList() {
    const { filtering, pagination } = useAppSelector((state) => state.log);
    const dispatch = useAppDispatch();

    // api
    const { data, error, isLoading } = logApi.useGetAllQuery({
        ...filtering.applied,
        ...pagination
    });
    const { data: count } = logApi.useGetCountQuery(filtering.applied);

    const changePagination = (page: number, pageSize: number) => {
        dispatch(actions.changePagination({ page, pageSize }));
    };

    return (
        <div>
            <LogFilter />
            <Table
                rowKey="_id"
                scroll={{ y: 800 }}
                loading={isLoading}
                dataSource={data}
                pagination={{
                    // position: ['topLeft'],
                    pageSizeOptions: [25, 50, 100, 200],
                    onChange: changePagination,
                    total: count?.count || 0,
                    showTotal: (total, range) =>
                        `${range[0]}-${range[1]} of ${total} items`,
                    pageSize: pagination.pageSize,
                    current: pagination.page
                }}
                columns={[
                    {
                        title: 'Timestamp',
                        key: 'created',
                        dataIndex: 'created',
                        render: (text: string, record) => (
                            <Link to={record._id}>
                                {dayjs(text).format('DD-MMM YY HH:mm:ss:SSS')}
                            </Link>
                        )
                    },
                    {
                        title: 'Level',
                        dataIndex: 'levelname',
                        key: 'levelname'
                        // filters: [
                        //     {
                        //         text: 'Joe',
                        //         value: 'Joe'
                        //     }
                        // ]
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
                        dataIndex: 'message',
                        key: 'message'
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
