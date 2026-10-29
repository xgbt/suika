import { useState } from 'react';
import { ConfigProvider, Layout, Typography, App as AntApp, Empty, Menu, theme } from 'antd';
import type { MenuProps } from 'antd';
import { ApartmentOutlined, VideoCameraFilled, VideoCameraOutlined } from '@ant-design/icons';
import zhCN from 'antd/locale/zh_CN';
import RoomList from './components/RoomList';
import AccountBar from './components/AccountBar';
import './App.css';

const { Header, Content, Sider } = Layout;
const { Title } = Typography;

function App() {
  const [activePage, setActivePage] = useState('recording');

  const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
    setActivePage(key);
  };

  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          borderRadius: 8,
          colorPrimary: '#6f8a79',
          colorInfo: '#6f8a79',
          colorSuccess: '#667f71',
          colorTextBase: '#1d1d1f',
          colorTextSecondary: '#6e6e73',
          colorBgContainer: '#fbfcfb',
          fontFamily: '"SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Noto Sans SC", "Microsoft YaHei", sans-serif',
        },
        components: {
          Button: { primaryShadow: 'none', defaultShadow: 'none' },
          Switch: { trackHeight: 28, trackMinWidth: 54, handleSize: 22, trackPadding: 3 },
        },
      }}
    >
      <AntApp>
        <Layout className="app-shell" hasSider>
          <Sider className="app-sider" width={220} theme="light">
            <div className="app-brand">
              <span className="app-brand-mark"><VideoCameraFilled /></span>
              <span>Suika</span>
            </div>
            <Menu
              className="app-nav"
              mode="inline"
              selectedKeys={[activePage]}
              onClick={handleMenuClick}
              items={[
                { key: 'recording', icon: <VideoCameraOutlined />, label: '录制管理' },
                { key: 'workflow', icon: <ApartmentOutlined />, label: '工作流' },
              ]}
            />
            <AccountBar />
          </Sider>
          <Layout className="app-main">
            <Header className="app-header">
              <Title level={4} className="app-title">
                {activePage === 'recording' ? '录制管理' : '工作流'}
              </Title>
            </Header>
            <Content className="app-content-wrap">
              {activePage === 'recording' ? (
                <div className="app-content-card">
                  <RoomList />
                </div>
              ) : (
                <div className="workflow-empty">
                  <Empty
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    description="工作流功能暂未开放"
                  />
                </div>
              )}
            </Content>
          </Layout>
        </Layout>
      </AntApp>
    </ConfigProvider>
  );
}
export default App
