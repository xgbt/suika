// Room API types matching room.proto

export enum LiveStatus {
  LIVE_STATUS_UNSPECIFIED = 0,
  LIVE_STATUS_PREPARING = 1,
  LIVE_STATUS_LIVE = 2,
}

export enum RecordStatus {
  RECORD_STATUS_UNSPECIFIED = 0,
  RECORD_STATUS_IDLE = 1,
  RECORD_STATUS_RECORDING = 2,
  RECORD_STATUS_REMUXING = 3,
  RECORD_STATUS_ERROR = 4,
}

export interface Room {
  room_id: number;
  name: string;
  enabled: boolean;
  live_status: LiveStatus;
  record_status: RecordStatus;
  current_file: string;
  bytes_written: number;
  session_started_at?: string;
  last_error: string;
  create_time?: string;
  update_time?: string;
}

async function request<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message ?? res.statusText);
  }
  return res.json();
}

export interface ListRoomsParams {
  page_size?: number;
  page_token?: string;
  room_id?: number;
  name?: string;
  enabled?: boolean;
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

  create(room: Pick<Room, 'room_id' | 'name' | 'enabled'>): Promise<{ room: Room }> {
    return request('/v1/rooms/create', { room });
  },

  update(
    room: Pick<Room, 'room_id'> & Partial<Pick<Room, 'name' | 'enabled'>>,
    paths: string[],
  ): Promise<{ room: Room }> {
    return request('/v1/rooms/update', {
      room,
      update_mask: { paths },
    });
  },

  delete(room_id: number): Promise<unknown> {
    return request('/v1/rooms/delete', { room_id });
  },
};
