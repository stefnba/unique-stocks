import { useAppSelector, useAppDispatch } from '@redux';
import { actions } from '@features/counter/slice';

import { Table } from 'antd';

import { useExampleQuery } from '@features/auth/api';

export default function Home() {
    const count = useAppSelector((state) => state.counter.value);
    const dispatch = useAppDispatch();
    const { data, error, isLoading } = useExampleQuery('bulbasaur');

    console.log(data);

    return (
        <div>
            <div>
                <button
                    aria-label="Increment value"
                    onClick={() => dispatch(actions.increment())}
                >
                    Increment
                </button>
                <span>{count}</span>
                <button
                    aria-label="Decrement value"
                    onClick={() => dispatch(actions.decrement())}
                >
                    Decrement
                </button>
            </div>
            <Table
                dataSource={data}
                columns={[
                    {
                        title: 'Name',
                        dataIndex: 'name',
                        key: 'name'
                    },
                    {
                        title: 'Mic',
                        dataIndex: 'mic',
                        key: 'mic'
                    },
                    {
                        title: 'Website',
                        dataIndex: 'website',
                        key: 'website'
                    }
                ]}
            />
        </div>
    );
}
