// Room API types matching room.proto

import { request } from './request';

export const LiveStatus = {
  LIVE_STATUS_UNSPECIFIED: 0,
  LIVE_STATUS_PREPARING: 1,
  LIVE_STATUS_LIVE: 2,
} as const;

export type LiveStatus = (typeof LiveStatus)[keyof typeof LiveStatus];

export const RecordStatus = {
  RECORD_STATUS_UNSPECIFIED: 0,
  RECORD_STATUS_IDLE: 1,
  RECORD_STATUS_RECORDING: 2,
  RECORD_STATUS_REMUXING: 3,
  RECORD_STATUS_ERROR: 4,
} as const;

export type RecordStatus = (typeof RecordStatus)[keyof typeof RecordStatus];

export interface Room {
  room_id: number;
  streamer_name: string;
  room_title: string;
  record_enabled: boolean;
  live_status: LiveStatus;
  record_status: RecordStatus;
  current_file: string;
  bytes_written: number;
  download_speed_bps: number;
  session_started_at?: string;
  last_error: string;
  create_time?: string;
  update_time?: string;
}

export interface ListRoomsParams {
  page_size?: number;
  page_token?: string;
  room_id?: number;
  streamer_name?: string;
  room_title?: string;
  record_enabled?: boolean;
}

export interface ListRoomsResponse {
  rooms: Room[];
  next_page_token: string;
}

export const roomsApi = {
  list(params: ListRoomsParams = {}): Promise<ListRoomsResponse> {
    return request('/v1/rooms/list', params);
  },

  get(room_id: number): Promise<{ room: Room }> {
    return request('/v1/rooms/get', { room_id });
  },

  create(room: Pick<Room, 'room_id' | 'record_enabled'>): Promise<{ room: Room }> {
    return request('/v1/rooms/create', { room });
  },

  updateRecordEnabled(room_id: number, record_enabled: boolean): Promise<{ room: Room }> {
    return request('/v1/rooms/update', {
      room: { room_id, record_enabled },
      update_mask: { paths: ['record_enabled'] },
    });
  },

  delete(room_id: number): Promise<unknown> {
    return request('/v1/rooms/delete', { room_id });
  },
};
