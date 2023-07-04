import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';

import { Button, Descriptions } from 'antd';

import { useGetOneQuery } from '@features/log/api';

import { useNavigate, useParams } from 'react-router-dom';

export default function LogOne() {
    const { id } = useParams();

    const { data, error, isLoading } = useGetOneQuery(id);

    const navigate = useNavigate();
    const goBack = () => {
        navigate(-1);
    };

    return (
        <div>
            <Button onClick={goBack}>Back</Button>
            {/* <pre>{JSON.stringify(data, undefined, 4)}</pre>
             */}
            {/* {prettyjson.render(
                data,
                {
                    keysColor: 'blue',
                    dashColor: 'magenta',
                    stringColor: 'white'
                },
                4
            )} */}
            {/* {<JSONPretty id="json-pretty" data={data}></JSONPretty>} */}
            <Descriptions title="Logging Info" layout="vertical" colon={false}>
                <Descriptions.Item label="Logger">
                    {`${data?.name} [${data?.service}]`}
                </Descriptions.Item>
                <Descriptions.Item label="Level">
                    {data?.levelname}
                </Descriptions.Item>
                <Descriptions.Item label="Time">
                    {dayjs(data?.created)
                        // .utcOffset(2 * 60)
                        .format('DD-MMM YY HH:mm:ss:SSS')}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Message"
                    span={3}
                    style={{
                        display: data?.message === null ? 'none' : null
                    }}
                >
                    {data?.message}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Event"
                    style={{
                        display: data?.event === null ? 'none' : null
                    }}
                >
                    {data?.event}
                </Descriptions.Item>
                <Descriptions.Item label="Filepath">
                    {data?.pathname}
                </Descriptions.Item>
                <Descriptions.Item label="Function">
                    {data?.funcName}
                </Descriptions.Item>
                <Descriptions.Item
                    label="DAG"
                    style={{
                        display: data?.dag_id === null ? 'none' : null
                    }}
                >
                    {data?.dag_id}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Task"
                    style={{
                        display: data?.task_id === null ? 'none' : null
                    }}
                >
                    {data?.task_id}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Run"
                    style={{
                        display: data?.run_id === null ? 'none' : null
                    }}
                >
                    {data?.run_id}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Extra"
                    style={{
                        display: data?.extra === null ? 'none' : null
                    }}
                >
                    <JSONPretty
                        id="json-pretty"
                        data={data?.extra}
                    ></JSONPretty>
                </Descriptions.Item>
            </Descriptions>
        </div>
    );
}
