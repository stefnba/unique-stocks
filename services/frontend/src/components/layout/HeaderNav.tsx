import {
    Layout,
    Menu,
    theme,
    Button,
    Input,
    Dropdown,
    message,
    Avatar
} from 'antd';

import {
    UserSwitchOutlined,
    SearchOutlined,
    NotificationOutlined,
    MenuFoldOutlined,
    MenuUnfoldOutlined,
    UploadOutlined,
    UserOutlined,
    VideoCameraOutlined
} from '@ant-design/icons';
import { FC } from 'react';
import type { MenuProps } from 'antd';
import { icons } from 'antd/es/image/PreviewGroup';

const { Header } = Layout;

const { Search } = Input;

type TopBarProps = {
    collapsed: boolean;
    setCollapsed: React.Dispatch<React.SetStateAction<boolean>>;
};
type CollapseButtonProps = TopBarProps;

const items: MenuProps['items'] = [
    {
        type: 'group',
        label: 'Stefan Bauer',
        children: [
            {
                label: 'Option 1',
                key: 'setting:1',
                icon: <UserSwitchOutlined />
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
    },
    {
        // label: 'Navigation One',
        key: 'mail',
        label: <UserSwitchOutlined />
    },
    {
        // label: 'Navigation Two',
        key: 'app',
        label: 'Logout',
        children: [
            {
                type: 'group',
                label: 'Item 1',
                children: [
                    {
                        label: 'Option 1',
                        key: 'setting:1',
                        icon: <UserSwitchOutlined />
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

const CollapseButton: React.FC<CollapseButtonProps> = ({
    collapsed,
    setCollapsed
}) => {
    return (
        <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            style={{
                fontSize: '16px',
                width: 64,
                height: 64
            }}
        />
    );
};

const handleMenuClick: MenuProps['onClick'] = (e) => {
    message.info('Click on menu item.');
    console.log('click', e);
};

const menuProps = {
    items
    // onClick: handleMenuClick
};

const TopBar: FC<TopBarProps> = ({ collapsed, setCollapsed }) => {
    return (
        <Header className="sticky bg-transparent w-full py-0 px-4 top-0 z-100 flex">
            {/* <Dropdown menu={menuProps}>asdf</Dropdown> */}
            {/* <Menu
                className="float-right w-full"
                items={items}
                mode="horizontal"
            /> */}
            <Button
                type="text"
                className="ml-auto"
                icon={<SearchOutlined />}
                // onClick={() => setCollapsed(!collapsed)}
                style={{
                    fontSize: '24px',
                    width: 64,
                    height: 64
                }}
            />
            <Button
                type="text"
                icon={<NotificationOutlined />}
                // onClick={() => setCollapsed(!collapsed)}
                style={{
                    fontSize: '24px',
                    width: 64,
                    height: 64
                }}
            />
            <Dropdown
                menu={{ items }}
                placement="bottomLeft"
                overlayClassName="w-60"
            >
                {/* <Avatar
                    shape="square"
                    size="large"
                    icon={<UserOutlined />}
                    className="cursor-pointer "
                /> */}
                <Button
                    type="text"
                    icon={<UserOutlined />}
                    // onClick={() => setCollapsed(!collapsed)}
                    style={{
                        fontSize: '24px',
                        width: 64,
                        height: 64
                    }}
                />
            </Dropdown>
        </Header>
    );
};

export default TopBar;
