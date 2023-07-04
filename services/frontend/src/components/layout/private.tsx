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
        <Layout>
            <Sider
                theme="light"
                breakpoint="lg"
                collapsedWidth="0"
                onBreakpoint={(broken) => {
                    console.log(broken);
                }}
                onCollapse={(collapsed, type) => {
                    console.log(collapsed, type);
                }}
                style={{
                    overflow: 'auto',
                    height: '100vh',
                    position: 'fixed',
                    left: 0,
                    top: 0,
                    bottom: 0
                }}
            >
                <div
                    style={{ margin: '48px 16px 0', backgroundImage: './' }}
                    className="demo-logo-vertical"
                />
                <SiderNavigation />
            </Sider>
            <Layout style={{ marginLeft: 200 }}>
                <Header
                    style={{
                        background: colorBgContainer,
                        padding: 0,
                        position: 'sticky',
                        top: 0,
                        zIndex: 1,
                        width: '100%',
                        display: 'flex',
                        alignItems: 'center'
                    }}
                />
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
                    Unique Stocks ©2023 Created by Stefan Bauer and Iraklis
                    Kordomatis
                </Footer>
            </Layout>
        </Layout>
    );
}
