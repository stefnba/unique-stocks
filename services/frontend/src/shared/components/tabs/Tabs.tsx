import { useNavigate, useParams } from 'react-router-dom';

import { Tabs as OriginalTabs } from 'antd';
import type { TabsProps as OriginalTabsProps } from 'antd';

type TabsProps = {
    keyName?: string;
    tabs: OriginalTabsProps['items'];
};

export default function Tabs({ keyName = 'key', tabs }: TabsProps) {
    const navigate = useNavigate();

    const params = useParams();

    const handleNavigate = (key: string) => {
        navigate(`../${key}`, {
            replace: true,
            relative: 'route'
        });
    };

    return (
        <OriginalTabs
            onTabClick={handleNavigate}
            activeKey={params[keyName]}
            defaultActiveKey={tabs[0]?.key}
            items={tabs}
        />
    );
}
