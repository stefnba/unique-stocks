import SelectFilter from '../../shared/components/filter/Select';

import { Col, Divider, Row } from 'antd';

const style: React.CSSProperties = { background: '#0092ff', padding: '8px 0' };

export default function LogFilter() {
    return (
        <>
            <Row gutter={16}>
                <Col className="gutter-row" span={6}>
                    <SelectFilter field={'levelname'} label="Level" />
                </Col>
                <Col className="gutter-row" span={6}>
                    <SelectFilter field="name" label="Logger" />
                </Col>
                <Col className="gutter-row" span={6}>
                    <SelectFilter field="event" label="Event" />
                </Col>
            </Row>
        </>
    );
}
