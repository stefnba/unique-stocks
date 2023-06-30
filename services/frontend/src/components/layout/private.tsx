import React from 'react';
import { Outlet } from 'react-router-dom';

import { Layout, Menu, theme } from 'antd';
import SiderNavigation from './nav/Navigation';

const { Header, Content, Footer, Sider } = Layout;

export default function PrivateLayout() {
    const {
        token: { colorBgContainer }
    } = theme.useToken();

    return (
        <Layout style={{ height: '100vh' }}>
            <Sider
                breakpoint="lg"
                collapsedWidth="0"
                onBreakpoint={(broken) => {
                    console.log(broken);
                }}
                onCollapse={(collapsed, type) => {
                    console.log(collapsed, type);
                }}
            >
                <div
                    style={{ margin: '24px 16px 0' }}
                    className="demo-logo-vertical"
                />
                <SiderNavigation />
            </Sider>
            <Layout>
                <Header style={{ padding: 0, background: colorBgContainer }} />
                <Content style={{ margin: '24px 16px 0' }}>
                    <div
                        style={{
                            padding: 24,
                            minHeight: 360,
                            background: colorBgContainer
                        }}
                    >
                        <Outlet />
                    </div>
                </Content>
                <Footer style={{ textAlign: 'center' }}>
                    Ant Design ©2023 Created by Ant UED
                </Footer>
            </Layout>
        </Layout>
    );
}
