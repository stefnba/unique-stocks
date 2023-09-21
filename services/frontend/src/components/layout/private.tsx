import { useState } from 'react';

import { Outlet } from 'react-router-dom';

import { Layout, theme } from 'antd';
import { MenuFoldOutlined, MenuUnfoldOutlined } from '@ant-design/icons';
import SiderNavigation from './nav/Navigation';
import TopBar from './HeaderNav';
import styled from 'styled-components';

const { Content, Footer, Sider } = Layout;

const StyledLayout = styled(Layout)`
    .ant-layout-sider {
        /* background: white; */
    }

    .ant-layout {
        background: white;
    }
    .ant-menu-light.ant-menu-root.ant-menu-inline,
    .ant-menu-light.ant-menu-root.ant-menu-vertical {
        border: none;
    }

    .ant-layout-sider-trigger {
        font-size: 18px;
    }
`;

import logo from '../../assets/logo.png';

export default function PrivateLayout() {
    const {
        token: { colorBgContainer }
    } = theme.useToken();

    const [collapsed, setCollapsed] = useState(true);

    return (
        <StyledLayout className="h-screen">
            <Sider
                theme="light"
                className="sticky bg-white"
                collapsible
                breakpoint="lg"
                onBreakpoint={(broken) => setCollapsed(broken)}
                defaultCollapsed={true}
                collapsedWidth="80"
                onCollapse={(collapsed) => setCollapsed(collapsed)}
                trigger={
                    collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />
                }
                collapsed={collapsed}
            >
                <div
                    style={{
                        height: 40,
                        backgroundImage: `url(${logo})`
                    }}
                    className="bg-contain bg-no-repeat cursor-pointer bg-center m-4"
                />
                <SiderNavigation />
            </Sider>
            <Layout>
                <TopBar collapsed={collapsed} setCollapsed={setCollapsed} />
                <Content
                    className="px-4"
                    style={{
                        overflow: 'auto'
                    }}
                >
                    <div
                        style={{
                            padding: 24,
                            minHeight: 'calc(100vh - 69px - 64px)',
                            background: colorBgContainer
                        }}
                    >
                        <Outlet />
                    </div>
                    <Footer className="text-center bg-white">
                        Unique Stocks ©2023 Created by Stefan Bauer and Iraklis
                        Kordomatis
                    </Footer>
                </Content>
            </Layout>
        </StyledLayout>
    );
}
