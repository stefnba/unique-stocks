import { Button, Input } from 'antd';
import { useState } from 'react';

const { Search } = Input;

export default function SearchFilter() {
    const [show, setShow] = useState(true);

    if (show) {
        return (
            <>
                <Search
                    placeholder="Search"
                    style={{ width: 400 }}
                    loading={false}
                    allowClear
                />
            </>
        );
    }
    return <Button onClick={() => setShow(true)}>ASda</Button>;
}
