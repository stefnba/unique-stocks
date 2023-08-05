import { Table, List, Typography } from 'antd';
// import dayjs from 'dayjs';
// import utc from 'dayjs/plugin/utc';
// import timezone from 'dayjs/plugin/timezone';

import { useAppSelector, useAppDispatch } from '@redux';

import * as exchangeApi from '../../features/security/api';

import { Link } from 'react-router-dom';

import Card from '@sharedComponents/card/Card';

const { Title } = Typography;
// import LogFilter from './Filter';

// dayjs.extend(utc);
// dayjs.extend(timezone);

export default function SecurityList() {
    // const { filtering, pagination } = useAppSelector((state) => state.exchange);

    // api
    const { data, isLoading } = exchangeApi.useGetAllSecurityQuery();

    return (
        <div>
            <Title>Security</Title>

            <List
                loading={isLoading}
                pagination={{
                    onChange: (page) => {
                        console.log(page);
                    },
                    pageSize: 20
                }}
                grid={{
                    gutter: 18,
                    xs: 1,
                    sm: 2,
                    md: 2,
                    lg: 3,
                    xl: 4,
                    xxl: 4
                }}
                dataSource={data}
                renderItem={(security) => (
                    <List.Item key={security.id}>
                        <Card
                            title={security.name}
                            link={`${security.id}`}
                            subTitle={security.ticker}
                        />
                    </List.Item>
                )}
            />
        </div>
    );
}
