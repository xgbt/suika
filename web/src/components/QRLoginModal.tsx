import { useCallback, useEffect, useRef, useState } from 'react';
import { App, Button, Modal, QRCode, Spin, Typography } from 'antd';
import { authApi, QRLoginStatus } from '../api/auth';
import type { QRLoginSession } from '../api/auth';

const { Text } = Typography;

const POLL_INTERVAL_MS = 2000;

interface QRLoginModalProps {
  open: boolean;
  onClose: () => void;
  onLoggedIn: () => void;
}

// QRLoginModal 展示扫码登录二维码并轮询登录状态。挂载即开始生成与轮询，
// 卸载（关闭弹窗）即停止。
function QRLoginModal({ open, onClose, onLoggedIn }: QRLoginModalProps) {
  const { message } = App.useApp();
  const [session, setSession] = useState<QRLoginSession | null>(null);
  const [status, setStatus] = useState<QRLoginStatus>(QRLoginStatus.QR_LOGIN_STATUS_NOT_SCANNED);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  // 终态（确认/过期）后停止轮询；用 ref 供定时器读取最新值。
  const statusRef = useRef(status);
  statusRef.current = status;
  const closedRef = useRef(false);

  // 生成二维码。依赖 nonce 以便"刷新二维码"时重跑；
  // cancelled 标志避免 StrictMode 双调用时的竞态。
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    closedRef.current = false;
    setSession(null);
    setStatus(QRLoginStatus.QR_LOGIN_STATUS_NOT_SCANNED);
    setError(null);
    authApi
      .createQRLogin()
      .then((s) => {
        if (!cancelled) setSession(s);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError((e as Error).message ?? '获取二维码失败');
      });
    return () => {
      cancelled = true;
    };
  }, [open, nonce]);

  // 轮询登录状态，直到终态或关闭。
  useEffect(() => {
    if (!open || !session) return;
    const qrcodeKey = session.qrcode_key;
    const timer = setInterval(async () => {
      const current = statusRef.current;
      if (
        closedRef.current ||
        current === QRLoginStatus.QR_LOGIN_STATUS_CONFIRMED ||
        current === QRLoginStatus.QR_LOGIN_STATUS_EXPIRED
      ) {
        return;
      }
      try {
        const { status: next } = await authApi.pollQRLogin(qrcodeKey);
        if (closedRef.current) return;
        setStatus(next);
        if (next === QRLoginStatus.QR_LOGIN_STATUS_CONFIRMED) {
          message.success('登录成功');
          onLoggedIn();
          onClose();
        }
      } catch (e: unknown) {
        if (!closedRef.current) setError((e as Error).message ?? '轮询失败');
      }
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [open, session, message, onLoggedIn, onClose]);

  const handleClose = useCallback(() => {
    closedRef.current = true;
    onClose();
  }, [onClose]);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  const expired = status === QRLoginStatus.QR_LOGIN_STATUS_EXPIRED;
  const scanned = status === QRLoginStatus.QR_LOGIN_STATUS_SCANNED;

  return (
    <Modal
      title="登录 B 站账号"
      open={open}
      onCancel={handleClose}
      footer={null}
      destroyOnHidden
      width={360}
    >
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, padding: '8px 0 4px' }}>
        {error ? (
          <>
            <Text type="danger">{error}</Text>
            <Button onClick={refresh}>重试</Button>
          </>
        ) : !session ? (
          <Spin style={{ margin: '48px 0' }} />
        ) : expired ? (
          <>
            <Text type="secondary">二维码已过期</Text>
            <Button type="primary" onClick={refresh}>
              刷新二维码
            </Button>
          </>
        ) : (
          <>
            <QRCode value={session.url} size={200} />
            <Text type="secondary">
              {scanned ? '已扫码，请在手机上确认' : '使用哔哩哔哩 App 扫码登录'}
            </Text>
          </>
        )}
      </div>
    </Modal>
  );
}

export default QRLoginModal;
