import { useNavigate, useMatches, useLocation } from 'react-router-dom';

import { Tabs as OriginalTabs } from 'antd';
import type { TabsProps as OriginalTabsProps } from 'antd';

type TabsProps = {
    basePath: string[];
    tabs: OriginalTabsProps['items'];
};

export default function Tabs({ tabs, basePath }: TabsProps) {
    const navigate = useNavigate();

    const location = useLocation();
    const matches = useMatches();

    const match = matches.filter((f) => f.pathname == location.pathname)[0];

    const handleTabClick = (key: string) => {
        navigate(`/${basePath.join('/')}/${key}`, {
            replace: true
        });
    };

    return (
        <OriginalTabs
            onTabClick={handleTabClick}
            activeKey={
                typeof match?.handle === 'object' && 'tabKey' in match?.handle
                    ? String(match?.handle['tabKey'])
                    : tabs[0]?.key
            }
            defaultActiveKey={tabs[0]?.key}
            items={tabs}
        />
    );
}
