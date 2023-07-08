import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Button, Col, Divider, Row, Space } from 'antd';
import queryString from 'query-string';

import { useAppSelector, useAppDispatch } from '@redux';
import { actions } from '@features/log/slice';
import SelectFilter from '../../shared/components/filter/Select';
import DateFilter from '../../shared/components/filter/Date';

const style: React.CSSProperties = { background: '#0092ff', padding: '8px 0' };

export default function LogFilter() {
    let [searchParams, setSearchParams] = useSearchParams();

    const { applied } = useAppSelector((state) => state.log.filtering);

    const dispatch = useAppDispatch();

    useEffect(() => {
        if (Object.keys(applied).length > 0) {
            setSearchParams(
                `?${queryString.stringify(applied, {
                    // arrayFormat: 'comma'
                })}`
            );
        }
    }, [applied]);

    useEffect(() => {
        dispatch(
            actions.applyFilter(
                queryString.parse(searchParams.toString(), {
                    // arrayFormat: 'comma'
                })
            )
        );
    }, []);

    const reset = () => {
        dispatch(actions.applyFilter({}));
        setSearchParams();
    };

    // ;

    return (
        // <div style={{ display: 'flex', gap: 10, marginBottom: 24 }}>
        <Space style={{ marginBottom: 24 }}>
            <DateFilter field="created" label="Timestamp" />
            <SelectFilter field="service" label="Service" />
            <SelectFilter field={'name'} label="Logger" />
            <SelectFilter field={'levelname'} label="Level" />
            <SelectFilter field="event" label="Event" />
            <SelectFilter field="dag_id" label="Dag" />
            <SelectFilter field="task_id" label="Task" />
            <SelectFilter field="run_id" label="Run" />
            {Object.keys(applied).length > 0 && (
                <Button type="text" onClick={reset}>
                    Reset
                </Button>
            )}
        </Space>
        // </div>
    );
}
