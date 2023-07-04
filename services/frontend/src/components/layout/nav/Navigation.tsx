import { Menu } from 'antd';
import {
    UploadOutlined,
    UserOutlined,
    VideoCameraOutlined
} from '@ant-design/icons';

import {
    AppstoreOutlined,
    MailOutlined,
    SettingOutlined
} from '@ant-design/icons';
import type { MenuProps } from 'antd';

const items: MenuProps['items'] = [
    {
        label: 'Navigation One',
        key: 'mail',
        icon: <MailOutlined />
    },
    {
        label: 'Navigation Two',
        key: 'app',
        icon: <AppstoreOutlined />,
        disabled: true
    },
    {
        label: 'Navigation Three - Submenu',
        key: 'SubMenu',
        icon: <SettingOutlined />,
        children: [
            {
                type: 'group',
                label: 'Item 1',
                children: [
                    {
                        label: 'Option 1',
                        key: 'setting:1'
                    },
                    {
                        label: 'Option 2',
                        key: 'setting:2'
                    }
                ]
            },
            {
                type: 'group',
                label: 'Item 2',
                children: [
                    {
                        label: 'Option 3',
                        key: 'setting:3'
                    },
                    {
                        label: 'Option 4',
                        key: 'setting:4'
                    }
                ]
            }
        ]
    }
];

export default function SiderNavigation() {
    return (
        <Menu
            // theme="dark"
            items={items}
            mode="inline"
            defaultSelectedKeys={['4']}
        />
    );
}
