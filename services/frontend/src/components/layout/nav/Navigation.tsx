import { Menu } from 'antd';
import {
    UploadOutlined,
    UserOutlined,
    VideoCameraOutlined
} from '@ant-design/icons';

import {
    AppstoreOutlined,
    MailOutlined,
    SettingOutlined,
    HomeOutlined
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { NavLink, useLocation } from 'react-router-dom';

const items: MenuProps['items'] = [
    {
        label: <NavLink to="/">Home</NavLink>,
        key: '/home#0',
        icon: <HomeOutlined />
    },
    {
        label: <NavLink to="/exchange">Exchange</NavLink>,
        key: '/exchange#0',
        icon: <AppstoreOutlined />
    },
    {
        label: <NavLink to="/entity">Entity</NavLink>,
        key: '/entity#0',
        icon: <SettingOutlined />
    },

    {
        label: <NavLink to="/index">Index</NavLink>,
        key: '/index#0',
        icon: <SettingOutlined />
    },
    {
        label: <NavLink to="/security">Security</NavLink>,
        key: '/security#0',
        icon: <SettingOutlined />
    },
    {
        label: <NavLink to="/log">Log</NavLink>,
        key: '/log#0',
        icon: <SettingOutlined />
    }
];

export default function SiderNavigation() {
    const location = useLocation();
    const paths = location.pathname.match(/\/\w+/g) || ['/home'];

    console.log(paths);
    return (
        <Menu
            selectedKeys={paths.map((value, index) => `${value}#${index}`)}
            items={items}
            mode="inline"
            defaultSelectedKeys={['/home']}
        />
    );
}
