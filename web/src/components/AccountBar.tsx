import { useCallback, useEffect, useState } from 'react';
import { App, Button, Space, Spin, Tag, Typography } from 'antd';
import { authApi, AccountState } from '../api/auth';
import type { AccountInfo } from '../api/auth';
import QRLoginModal from './QRLoginModal';

const { Text } = Typography;

// AccountBar 位于页头右侧，展示登录状态并提供登录/登出入口。
function AccountBar() {
  const { message, modal } = App.useApp();
  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [loginOpen, setLoginOpen] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      const { account: info } = await authApi.getAccountStatus();
      setAccount(info);
    } catch (e: unknown) {
      // 平台不可达时退回登出视图，避免阻塞页头渲染。
      setAccount({ state: AccountState.ACCOUNT_STATE_LOGGED_OUT, uname: '', mid: 0 });
      message.error((e as Error).message ?? '获取账号状态失败');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const handleLogout = useCallback(() => {
    modal.confirm({
      title: '退出登录',
      content: '将清除本机保存的 B 站凭据，录制将无法使用登录态。确定退出？',
      okText: '退出',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: async () => {
        try {
          await authApi.logout();
          message.success('已退出登录');
          await refresh();
        } catch (e: unknown) {
          message.error((e as Error).message ?? '退出登录失败');
        }
      },
    });
  }, [modal, message, refresh]);

  const state = account?.state;

  return (
    <>
      <Space size="middle">
        {loading && !account ? (
          <Spin size="small" />
        ) : state === AccountState.ACCOUNT_STATE_LOGGED_IN ? (
          <>
            <Text strong>已登录：{account?.uname || '未知用户'}</Text>
            <Button size="small" onClick={handleLogout}>
              退出
            </Button>
          </>
        ) : state === AccountState.ACCOUNT_STATE_EXPIRED ? (
          <>
            <Tag color="warning">登录已过期</Tag>
            <Button size="small" type="primary" onClick={() => setLoginOpen(true)}>
              重新登录
            </Button>
          </>
        ) : (
          <Button size="small" type="primary" onClick={() => setLoginOpen(true)}>
            登录
          </Button>
        )}
      </Space>
      {loginOpen && (
        <QRLoginModal
          open={loginOpen}
          onClose={() => setLoginOpen(false)}
          onLoggedIn={() => void refresh()}
        />
      )}
    </>
  );
}

export default AccountBar;
