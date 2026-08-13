import { ConfigProvider, Layout, Typography, App as AntApp, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import RoomList from './components/RoomList';

const { Header, Content } = Layout;
const { Title } = Typography;

function App() {
  return (
    <ConfigProvider locale={zhCN} theme={{ algorithm: theme.defaultAlgorithm }}>
      <AntApp>
        <Layout style={{ minHeight: '100vh' }}>
          <Header style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <Title level={4} style={{ color: '#fff', margin: 0 }}>
              🎙 Suika 直播录制管理
            </Title>
          </Header>
          <Content style={{ padding: '24px 32px', maxWidth: 1200, margin: '0 auto', width: '100%' }}>
            <RoomList />
          </Content>
        </Layout>
      </AntApp>
    </ConfigProvider>
  );
}
export default App
