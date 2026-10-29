import { useCallback, useEffect, useState } from 'react';
import { App, Avatar, Popover, Spin } from 'antd';
import { LogoutOutlined, UserOutlined } from '@ant-design/icons';
import { authApi, AccountState } from '../api/auth';
import type { AccountInfo } from '../api/auth';
import QRLoginModal from './QRLoginModal';

// AccountBar 位于侧栏底部，展示登录状态；已登录时悬停头像弹出退出入口。
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
  const loggedIn = state === AccountState.ACCOUNT_STATE_LOGGED_IN;
  const expired = state === AccountState.ACCOUNT_STATE_EXPIRED;
  const name = loggedIn ? account?.uname || '未知用户' : expired ? '登录已过期' : '未登录';
  const hint = loggedIn ? '已登录' : expired ? '点击重新登录' : '点击登录';

  const popoverContent = loggedIn ? (
    <button type="button" className="account-action account-action-danger" onClick={handleLogout}>
      <LogoutOutlined />
      退出登录
    </button>
  ) : null;

  const trigger = (
    <div
      className={`account-card${loggedIn ? '' : ' account-card-clickable'}`}
      role={loggedIn ? undefined : 'button'}
      tabIndex={loggedIn ? undefined : 0}
      onClick={loggedIn ? undefined : () => setLoginOpen(true)}
      onKeyDown={loggedIn ? undefined : (e) => e.key === 'Enter' && setLoginOpen(true)}
    >
      <Avatar className={`account-avatar${loggedIn ? '' : ' account-avatar-off'}`} size={36}>
        {loggedIn ? name.slice(0, 1).toUpperCase() : <UserOutlined />}
      </Avatar>
      <div className="account-meta">
        <span className="account-name">{name}</span>
        <span className={`account-hint${expired ? ' account-hint-warn' : ''}`}>{hint}</span>
      </div>
    </div>
  );

  return (
    <>
      {loading && !account ? (
        <div className="account-card">
          <Spin size="small" />
        </div>
      ) : (
        <Popover
          content={popoverContent}
          placement="topLeft"
          trigger="hover"
          arrow={false}
          overlayClassName="account-popover"
          open={loggedIn ? undefined : false}
        >
          {trigger}
        </Popover>
      )}
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
