import { Table, List, Typography } from 'antd';
// import dayjs from 'dayjs';
// import utc from 'dayjs/plugin/utc';
// import timezone from 'dayjs/plugin/timezone';

import { useAppSelector, useAppDispatch } from '@redux';

import * as exchangeApi from '../../features/exchange/api';
// import { actions } from '@features/exchange/slice';

import { Link } from 'react-router-dom';
import ExchangeFilter from './Filter';
import Card from '@sharedComponents/card/Card';

const { Title } = Typography;
// import LogFilter from './Filter';

// dayjs.extend(utc);
// dayjs.extend(timezone);

export default function ExchangeList() {
    // const { filtering, pagination } = useAppSelector((state) => state.exchange);

    // api
    const { data, isLoading } = exchangeApi.useGetAllExchangeQuery();
    // const { data: count } = logApi.useGetCountQuery(filtering.applied);

    // const changePagination = (page: number, pageSize: number) => {
    //     dispatch(actions.changePagination({ page, pageSize }));
    // };

    return (
        <div>
            <Title>Exchange</Title>
            {/* <ExchangeFilter /> */}
            <List
                // itemLayout="vertical"
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
                renderItem={(exchange) => (
                    <List.Item key={exchange.id}>
                        {/* <Link to={`${exchange.id}`}> */}
                        <Card
                            title={exchange.name}
                            link={`${exchange.id}`}
                            subTitle={exchange.mic}
                            // tags={[exchange.source]}
                        />
                        {/* </Link> */}
                        {/* <List.Item.Meta
                            // avatar={<Avatar src={item.avatar} />}
                            title={
                                <Link to={`${exchange.id}`}>
                                    {exchange.name}
                                </Link>
                            }

                            // description={item.description}
                        /> */}
                    </List.Item>
                )}
            />
        </div>
    );
}
