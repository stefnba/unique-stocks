import { Input, Space } from 'antd';
import SelectFilter from '../../shared/components/filter/Select';

const { Search } = Input;

export default function ExchangeFilter() {
    return (
        <>
            <Space>
                <Search
                    placeholder="Search Exchanges"
                    allowClear
                    // onSearch={onSearch}
                    style={{ width: 304 }}
                />
                {/* <SelectFilter field="created" label="Timestamp" /> */}
            </Space>
        </>
    );
}
