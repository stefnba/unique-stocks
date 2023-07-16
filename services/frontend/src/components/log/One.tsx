import JSONPretty from 'react-json-pretty';
import dayjs from 'dayjs';
import prettyjson from 'prettyjson';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { docco } from 'react-syntax-highlighter/dist/cjs/styles/hljs';

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

    const {
        _id,
        asctime,
        dag_id,
        event,
        exc_text,
        filename,
        module,
        funcName,
        message,
        pathname,
        created,
        lineno,
        run_id,
        task_id,
        levelname,
        name,
        service,
        query,
        ...extra
    } = data || {};

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
                    {`${name} [${service}]`}
                </Descriptions.Item>
                <Descriptions.Item label="Level">{levelname}</Descriptions.Item>
                <Descriptions.Item label="Time">
                    {dayjs(created)
                        // .utcOffset(2 * 60)
                        .format('DD-MMM YY HH:mm:ss:SSS')}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Message"
                    span={3}
                    style={{
                        display: message === null ? 'none' : null
                    }}
                >
                    {message}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Event"
                    style={{
                        display: event === null ? 'none' : null
                    }}
                >
                    {event}
                </Descriptions.Item>
                <Descriptions.Item label="Origination">
                    {`${funcName}() @ "${pathname}" [line: ${lineno}]`}
                </Descriptions.Item>
                {/* <Descriptions.Item label="Function">
                    {funcName}
                </Descriptions.Item> */}
                <Descriptions.Item
                    label="DAG"
                    style={{
                        display: dag_id === null ? 'none' : null
                    }}
                >
                    {dag_id}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Task"
                    style={{
                        display: task_id === null ? 'none' : null
                    }}
                >
                    {task_id}
                </Descriptions.Item>
                <Descriptions.Item
                    label="Run"
                    style={{
                        display: run_id === null ? 'none' : null
                    }}
                >
                    {run_id}
                </Descriptions.Item>
                <Descriptions.Item
                    span={2}
                    label="Stack"
                    style={{
                        display: exc_text === null ? 'none' : null,
                        whiteSpace: 'pre-wrap'
                    }}
                >
                    {exc_text}
                </Descriptions.Item>
                <Descriptions.Item
                    span={3}
                    label="Query"
                    style={{
                        display: query === null ? 'none' : null,
                        whiteSpace: 'pre-wrap'
                    }}
                >
                    {}

                    <SyntaxHighlighter
                        showLineNumbers={false}
                        language="sql"
                        style={docco}
                        customStyle={{
                            background: 'transparent',
                            fontSize: 14
                        }}
                    >
                        {query}
                    </SyntaxHighlighter>
                </Descriptions.Item>
                <Descriptions.Item
                    label="Extra"
                    style={{
                        display: extra === null ? 'none' : null
                    }}
                >
                    {/* <JSONPretty id="json-pretty" data={extra}></JSONPretty>
                     */}
                    <SyntaxHighlighter
                        language="json"
                        style={docco}
                        customStyle={{
                            background: 'transparent',
                            fontSize: 14
                        }}
                    >
                        {JSON.stringify(extra, null, 4)}
                    </SyntaxHighlighter>
                </Descriptions.Item>
            </Descriptions>
        </div>
    );
}
