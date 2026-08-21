import React, { useEffect, useState, useCallback, useRef, useId } from 'react';
import {
  Card,
  Button,
  Modal,
  Form,
  Input,
  InputNumber,
  Switch,
  Space,
  Tag,
  Tooltip,
  Popconfirm,
  Typography,
  App,
  Badge,
  Empty,
  Spin,
} from 'antd';
import {
  PlusOutlined,
  ReloadOutlined,
  EditOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { roomsApi, LiveStatus, RecordStatus } from '../api/rooms';
import type { Room } from '../api/rooms';
import './RoomList.css';

const { Text } = Typography;

function formatBytes(bytes: number): string {
  if (!bytes) return '—';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function formatSpeed(bytesPerSecond: number): string {
  if (!bytesPerSecond) return '—';
  return `${formatBytes(bytesPerSecond)}/s`;
}

const RECORD_STATUS_MAP: Record<RecordStatus, React.ReactNode> = {
  [RecordStatus.RECORD_STATUS_UNSPECIFIED]: <Tag>未知</Tag>,
  [RecordStatus.RECORD_STATUS_IDLE]: <Tag color="default">空闲</Tag>,
  [RecordStatus.RECORD_STATUS_RECORDING]: <Badge status="processing" color="green" text={<Text type="success">录制中</Text>} />,
  [RecordStatus.RECORD_STATUS_REMUXING]: <Badge status="processing" color="blue" text={<Text type="secondary">合并中</Text>} />,
  [RecordStatus.RECORD_STATUS_ERROR]: <Tag color="error">错误</Tag>,
};

type ModalMode = 'create' | 'edit';
const SPEED_HISTORY_POINTS = 24;

type SpeedSparklineProps = {
  speeds: number[];
};

function SpeedSparkline({ speeds }: SpeedSparklineProps) {
  const gradientId = useId().replace(/:/g, '_');
  const width = 220;
  const height = 72;
  const padding = 8;
  const max = Math.max(1, ...speeds);
  const denominator = Math.max(1, speeds.length - 1);
  const points = speeds.map((value, idx) => {
    const x = padding + (idx / denominator) * (width - padding * 2);
    const y = height - padding - (value / max) * (height - padding * 2);
    return `${x},${y}`;
  });
  const areaPoints = [`${padding},${height - padding}`, ...points, `${width - padding},${height - padding}`].join(' ');

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="speed-chart" role="img" aria-label="下载速度折线图">
      <defs>
        <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="rgba(123, 149, 136, 0.10)" />
          <stop offset="100%" stopColor="rgba(123, 149, 136, 0.01)" />
        </linearGradient>
      </defs>
      <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} className="speed-chart-axis" />
      <polygon points={areaPoints} fill={`url(#${gradientId})`} />
      <polyline points={points.join(' ')} className="speed-chart-line" fill="none" />
      {points.length > 0 ? <circle cx={width - padding} cy={Number(points[points.length - 1].split(',')[1])} r="3" className="speed-chart-dot" /> : null}
    </svg>
  );
}

function SpeedTooltipChart({ speeds }: SpeedSparklineProps) {
  return (
    <div className="speed-tooltip-panel">
      <div className="speed-tooltip-title">下载速度趋势</div>
      <SpeedSparkline speeds={speeds} />
    </div>
  );
}

export default function RoomList() {
  const { message, modal } = App.useApp();

  const [rooms, setRooms] = useState<Room[]>([]);
  const [loading, setLoading] = useState(false);
  const [nextPageToken, setNextPageToken] = useState<string>('');
  const [pageTokenStack, setPageTokenStack] = useState<string[]>(['']);
  const [currentPage, setCurrentPage] = useState(0);
  const [speedHistoryByRoom, setSpeedHistoryByRoom] = useState<Record<number, number[]>>({});

  const [modalOpen, setModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<ModalMode>('create');
  const [editingRoom, setEditingRoom] = useState<Room | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const [form] = Form.useForm();
  const PAGE_SIZE = 20;
  const REFRESH_INTERVAL_MS = 2000;

  const loadPage = useCallback(async (token: string) => {
    setLoading(true);
    try {
      const res = await roomsApi.list({ page_size: PAGE_SIZE, page_token: token || undefined });
      const nextRooms = res.rooms ?? [];
      setRooms(nextRooms);
      setSpeedHistoryByRoom((prev) => {
        const next: Record<number, number[]> = {};
        for (const room of nextRooms) {
          const history = prev[room.room_id] ?? [];
          const merged = [...history, room.download_speed_bps ?? 0];
          if (merged.length > SPEED_HISTORY_POINTS) {
            merged.splice(0, merged.length - SPEED_HISTORY_POINTS);
          }
          next[room.room_id] = merged;
        }
        return next;
      });
      setNextPageToken(res.next_page_token ?? '');
    } catch (e: unknown) {
      message.error((e as Error).message ?? '加载失败');
    } finally {
      setLoading(false);
    }
  }, [message]);

  // Auto-refresh every 2 seconds
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tokenRef = useRef<string>('');

  useEffect(() => {
    tokenRef.current = pageTokenStack[currentPage] ?? '';
    loadPage(tokenRef.current);
  }, [currentPage, pageTokenStack, loadPage]);

  useEffect(() => {
    timerRef.current = setInterval(() => {
      loadPage(tokenRef.current);
    }, REFRESH_INTERVAL_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [loadPage, REFRESH_INTERVAL_MS]);

  function handleRefresh() {
    loadPage(pageTokenStack[currentPage] ?? '');
  }

  function handlePrevPage() {
    if (currentPage === 0) return;
    setCurrentPage((p) => p - 1);
  }

  function handleNextPage() {
    if (!nextPageToken) return;
    setPageTokenStack((stack) => {
      const next = [...stack];
      next[currentPage + 1] = nextPageToken;
      return next;
    });
    setCurrentPage((p) => p + 1);
  }

  function openCreate() {
    setModalMode('create');
    setEditingRoom(null);
    form.resetFields();
    form.setFieldsValue({ enabled: true });
    setModalOpen(true);
  }

  function openEdit(room: Room) {
    setModalMode('edit');
    setEditingRoom(room);
    form.setFieldsValue({
      streamer_name: room.streamer_name,
      room_title: room.room_title,
      enabled: room.enabled,
    });
    setModalOpen(true);
  }

  async function handleSubmit() {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      if (modalMode === 'create') {
        await roomsApi.create({
          room_id: values.room_id,
          streamer_name: values.streamer_name,
          room_title: values.room_title,
          enabled: values.enabled ?? false,
        });
        message.success('添加成功');
      } else if (editingRoom) {
        const paths: string[] = [];
        if (values.streamer_name !== editingRoom.streamer_name) paths.push('streamer_name');
        if (values.room_title !== editingRoom.room_title) paths.push('room_title');
        if (values.enabled !== editingRoom.enabled) paths.push('enabled');
        if (paths.length === 0) {
          message.info('没有改动');
          setModalOpen(false);
          return;
        }
        await roomsApi.update(
          {
            room_id: editingRoom.room_id,
            streamer_name: values.streamer_name,
            room_title: values.room_title,
            enabled: values.enabled,
          },
          paths,
        );
        message.success('更新成功');
      }
      setModalOpen(false);
      loadPage(pageTokenStack[currentPage] ?? '');
    } catch (e: unknown) {
      message.error((e as Error).message ?? '操作失败');
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete(room_id: number) {
    try {
      await roomsApi.delete(room_id);
      message.success('删除成功');
      loadPage(pageTokenStack[currentPage] ?? '');
    } catch (e: unknown) {
      message.error((e as Error).message ?? '删除失败');
    }
  }

  function openRoomWithConfirm(roomID: number) {
    window.open(`https://live.bilibili.com/${roomID}`, '_blank', 'noopener,noreferrer');
  }

  return (
    <>
      <div className="room-toolbar">
        <Space>
          <Button icon={<PlusOutlined />} type="primary" onClick={openCreate}>
            添加房间
          </Button>
          <Button icon={<ReloadOutlined />} onClick={handleRefresh} loading={loading}>
            刷新
          </Button>
        </Space>
        <Space>
          <Button size="small" disabled={currentPage === 0} onClick={handlePrevPage}>
            上一页
          </Button>
          <Text type="secondary">第 {currentPage + 1} 页</Text>
          <Button size="small" disabled={!nextPageToken} onClick={handleNextPage}>
            下一页
          </Button>
        </Space>
      </div>

      <Spin spinning={loading}>
        {rooms.length === 0 ? (
          <Card className="room-card-empty" bordered={false}>
            <Empty description="当前页暂无房间" />
          </Card>
        ) : (
          <div className="room-grid">
            {rooms.map((room) => {
              const speedHistory = speedHistoryByRoom[room.room_id] ?? [0];
              const isLive = room.live_status === LiveStatus.LIVE_STATUS_LIVE;
              const isRecording = room.record_status === RecordStatus.RECORD_STATUS_RECORDING;
              const livePillClassName = isRecording ? 'live-pill live-pill-strong' : 'live-pill live-pill-light';
              return (
                <Card key={room.room_id} className={`room-card ${isLive ? 'room-card-live' : ''}`} bordered={false}>
                  <div className="room-card-head">
                    <div>
                      <div className="room-link-row">
                        <Popconfirm
                          title={`确认打开房间 ${room.room_id}？`}
                          description="将在新标签页跳转到 Bilibili 直播间"
                          okText="打开"
                          cancelText="取消"
                          onConfirm={() => openRoomWithConfirm(room.room_id)}
                        >
                          <button type="button" className="room-link room-link-trigger">
                            房间 #{room.room_id}
                          </button>
                        </Popconfirm>
                        {isLive ? <span className={livePillClassName}>LIVE</span> : null}
                      </div>
                      <div className="room-name">{room.streamer_name || '未命名主播'}</div>
                    </div>
                    <Space align="start" className="room-head-actions">
                      <Tooltip title="编辑">
                        <Button type="text" size="small" icon={<EditOutlined />} onClick={() => openEdit(room)} />
                      </Tooltip>
                      <Popconfirm
                        title={`确认删除房间 ${room.room_id}？`}
                        onConfirm={() => handleDelete(room.room_id)}
                        okText="删除"
                        cancelText="取消"
                        okButtonProps={{ danger: true }}
                      >
                        <Tooltip title="删除">
                          <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                        </Tooltip>
                      </Popconfirm>
                    </Space>
                  </div>

                  <div className="room-title">{room.room_title || '暂无房间标题'}</div>

                  <div className="room-status-row">
                    {RECORD_STATUS_MAP[room.record_status] ?? <Tag>未知</Tag>}
                    <span className="room-toggle">
                      启用
                      <Switch
                        size="small"
                        checked={room.enabled}
                        onChange={(checked) => {
                          modal.confirm({
                            title: checked ? `启用房间 ${room.room_id}？` : `禁用房间 ${room.room_id}？`,
                            okText: checked ? '启用' : '禁用',
                            cancelText: '取消',
                            okButtonProps: checked ? {} : { danger: true },
                            onOk: async () => {
                              try {
                                await roomsApi.update({ room_id: room.room_id, enabled: checked }, ['enabled']);
                                loadPage(pageTokenStack[currentPage] ?? '');
                              } catch (e: unknown) {
                                message.error((e as Error).message ?? '更新失败');
                              }
                            },
                          });
                        }}
                      />
                    </span>
                  </div>

                  <div className="room-metrics">
                    <div className="metric-box">
                      <span>已录制</span>
                      <strong>{formatBytes(room.bytes_written)}</strong>
                    </div>
                    <div className="metric-box">
                      <span>实时下载速度</span>
                      <Tooltip placement="top" title={<SpeedTooltipChart speeds={speedHistory} />}>
                        <strong className={`speed-chip ${isRecording ? 'speed-chip-live' : ''}`}>{formatSpeed(room.download_speed_bps)}</strong>
                      </Tooltip>
                    </div>
                  </div>

                  <div className="room-foot">
                    <Text type="secondary" ellipsis={{ tooltip: room.current_file || '暂无录制文件' }}>
                      文件: {room.current_file || '—'}
                    </Text>
                    {room.last_error ? (
                      <Tooltip title={room.last_error}>
                        <Text type="danger" ellipsis>
                          错误: {room.last_error}
                        </Text>
                      </Tooltip>
                    ) : (
                      <Text type="secondary">错误: —</Text>
                    )}
                  </div>
                </Card>
              );
            })}
          </div>
        )}
      </Spin>

      <Modal
        title={modalMode === 'create' ? '添加房间' : '编辑房间'}
        open={modalOpen}
        onOk={handleSubmit}
        onCancel={() => setModalOpen(false)}
        confirmLoading={submitting}
        okText={modalMode === 'create' ? '添加' : '保存'}
        cancelText="取消"
        destroyOnHidden
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          {modalMode === 'create' && (
            <Form.Item
              label="房间 ID"
              name="room_id"
              rules={[
                { required: true, message: '请输入房间 ID' },
                { type: 'number', min: 1, message: '房间 ID 须为正整数' },
              ]}
            >
              <InputNumber style={{ width: '100%' }} placeholder="Bilibili 直播间 ID" min={1} precision={0} />
            </Form.Item>
          )}
          <Form.Item label="主播名称" name="streamer_name">
            <Input placeholder="可选，留空则由平台自动填充" allowClear />
          </Form.Item>
          <Form.Item label="房间标题" name="room_title">
            <Input placeholder="可选，留空则由平台自动填充" allowClear />
          </Form.Item>
          <Form.Item label="启用录制" name="enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
