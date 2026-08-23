import { ConfigProvider, Layout, Typography, App as AntApp, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import RoomList from './components/RoomList';
import AccountBar from './components/AccountBar';
import './App.css';

const { Header, Content } = Layout;
const { Title } = Typography;

function App() {
  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          borderRadius: 12,
          colorPrimary: '#6f8a79',
          colorInfo: '#6f8a79',
          colorSuccess: '#667f71',
          colorTextBase: '#1d1d1f',
          colorTextSecondary: '#6e6e73',
          colorBgContainer: '#fbfcfb',
          fontFamily: '"SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Noto Sans SC", "Microsoft YaHei", sans-serif',
        },
      }}
    >
      <AntApp>
        <Layout className="app-shell">
          <Header className="app-header">
            <Title level={4} className="app-title">
              Suika 直播录制管理
            </Title>
            <div className="app-header-right">
              <AccountBar />
            </div>
          </Header>
          <Content className="app-content-wrap">
            <div className="app-content-card">
              <RoomList />
            </div>
          </Content>
        </Layout>
      </AntApp>
    </ConfigProvider>
  );
}
export default App
