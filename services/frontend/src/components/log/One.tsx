import JSONPretty from 'react-json-pretty';
import prettyjson from 'prettyjson';

import { Button } from 'antd';

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
            {<JSONPretty id="json-pretty" data={data}></JSONPretty>}
        </div>
    );
}
