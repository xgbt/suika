// Account API types matching account.proto

import { request } from './request';

export const QRLoginStatus = {
  QR_LOGIN_STATUS_UNSPECIFIED: 0,
  QR_LOGIN_STATUS_NOT_SCANNED: 1,
  QR_LOGIN_STATUS_SCANNED: 2,
  QR_LOGIN_STATUS_EXPIRED: 3,
  QR_LOGIN_STATUS_CONFIRMED: 4,
} as const;

export type QRLoginStatus = (typeof QRLoginStatus)[keyof typeof QRLoginStatus];

export const AccountState = {
  ACCOUNT_STATE_UNSPECIFIED: 0,
  ACCOUNT_STATE_LOGGED_OUT: 1,
  ACCOUNT_STATE_LOGGED_IN: 2,
  ACCOUNT_STATE_EXPIRED: 3,
} as const;

export type AccountState = (typeof AccountState)[keyof typeof AccountState];

export interface AccountInfo {
  state: AccountState;
  uname: string;
  mid: number;
}

export interface QRLoginSession {
  url: string;
  qrcode_key: string;
  expire_time?: string;
}

export const authApi = {
  createQRLogin(): Promise<QRLoginSession> {
    return request('/v1/account/qr-login/create', {});
  },

  pollQRLogin(qrcode_key: string): Promise<{ status: QRLoginStatus }> {
    return request('/v1/account/qr-login/poll', { qrcode_key });
  },

  getAccountStatus(): Promise<{ account: AccountInfo }> {
    return request('/v1/account/status/get', {});
  },

  logout(): Promise<unknown> {
    return request('/v1/account/logout', {});
  },
};
