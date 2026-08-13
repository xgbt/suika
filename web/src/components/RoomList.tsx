import React, { useEffect, useState, useCallback, useRef } from 'react';
import {
  Table,
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
} from 'antd';
import {
  PlusOutlined,
  ReloadOutlined,
  EditOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { roomsApi, LiveStatus, RecordStatus } from '../api/rooms';
import type { Room } from '../api/rooms';

const { Text } = Typography;

function formatBytes(bytes: number): string {
  if (!bytes) return '—';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

const LIVE_STATUS_MAP: Record<LiveStatus, React.ReactNode> = {
  [LiveStatus.LIVE_STATUS_UNSPECIFIED]: <Tag>未知</Tag>,
  [LiveStatus.LIVE_STATUS_PREPARING]: <Tag color="default">未开播</Tag>,
  [LiveStatus.LIVE_STATUS_LIVE]: <Badge status="processing" color="red" text={<Text type="danger">直播中</Text>} />,
};

const RECORD_STATUS_MAP: Record<RecordStatus, React.ReactNode> = {
  [RecordStatus.RECORD_STATUS_UNSPECIFIED]: <Tag>未知</Tag>,
  [RecordStatus.RECORD_STATUS_IDLE]: <Tag color="default">空闲</Tag>,
  [RecordStatus.RECORD_STATUS_RECORDING]: <Badge status="processing" color="green" text={<Text type="success">录制中</Text>} />,
  [RecordStatus.RECORD_STATUS_REMUXING]: <Badge status="processing" color="blue" text={<Text type="secondary">合并中</Text>} />,
  [RecordStatus.RECORD_STATUS_ERROR]: <Tag color="error">错误</Tag>,
};

type ModalMode = 'create' | 'edit';

export default function RoomList() {
  const { message, modal } = App.useApp();

  const [rooms, setRooms] = useState<Room[]>([]);
  const [loading, setLoading] = useState(false);
  const [nextPageToken, setNextPageToken] = useState<string>('');
  const [pageTokenStack, setPageTokenStack] = useState<string[]>(['']);
  const [currentPage, setCurrentPage] = useState(0);

  const [modalOpen, setModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<ModalMode>('create');
  const [editingRoom, setEditingRoom] = useState<Room | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const [form] = Form.useForm();
  const PAGE_SIZE = 20;

  const loadPage = useCallback(async (token: string) => {
    setLoading(true);
    try {
      const res = await roomsApi.list({ page_size: PAGE_SIZE, page_token: token || undefined });
      setRooms(res.rooms ?? []);
      setNextPageToken(res.next_page_token ?? '');
    } catch (e: unknown) {
      message.error((e as Error).message ?? '加载失败');
    } finally {
      setLoading(false);
    }
  }, [message]);

  // Auto-refresh every 5 seconds
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tokenRef = useRef<string>('');

  useEffect(() => {
    tokenRef.current = pageTokenStack[currentPage] ?? '';
    loadPage(tokenRef.current);
  }, [currentPage, pageTokenStack, loadPage]);

  useEffect(() => {
    timerRef.current = setInterval(() => {
      loadPage(tokenRef.current);
    }, 5000);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [loadPage]);

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
    form.setFieldsValue({ name: room.name, enabled: room.enabled });
    setModalOpen(true);
  }

  async function handleSubmit() {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      if (modalMode === 'create') {
        await roomsApi.create({
          room_id: values.room_id,
          name: values.name,
          enabled: values.enabled ?? false,
        });
        message.success('添加成功');
      } else if (editingRoom) {
        const paths: string[] = [];
        if (values.name !== editingRoom.name) paths.push('name');
        if (values.enabled !== editingRoom.enabled) paths.push('enabled');
        if (paths.length === 0) {
          message.info('没有改动');
          setModalOpen(false);
          return;
        }
        await roomsApi.update(
          { room_id: editingRoom.room_id, name: values.name, enabled: values.enabled },
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

  const columns: ColumnsType<Room> = [
    {
      title: '房间 ID',
      dataIndex: 'room_id',
      width: 110,
      render: (id: number) => (
        <a href={`https://live.bilibili.com/${id}`} target="_blank" rel="noreferrer">
          {id}
        </a>
      ),
    },
    {
      title: '名称',
      dataIndex: 'name',
      ellipsis: true,
      render: (name: string) => name || <Text type="secondary">—</Text>,
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      width: 70,
      render: (v: boolean, record: Room) => (
        <Switch
          size="small"
          checked={v}
          onChange={(checked) => {
            modal.confirm({
              title: checked ? `启用房间 ${record.room_id}？` : `禁用房间 ${record.room_id}？`,
              okText: checked ? '启用' : '禁用',
              cancelText: '取消',
              okButtonProps: checked ? {} : { danger: true },
              onOk: async () => {
                try {
                  await roomsApi.update({ room_id: record.room_id, enabled: checked }, ['enabled']);
                  loadPage(pageTokenStack[currentPage] ?? '');
                } catch (e: unknown) {
                  message.error((e as Error).message ?? '更新失败');
                }
              },
            });
          }}
        />
      ),
    },
    {
      title: '直播状态',
      dataIndex: 'live_status',
      width: 100,
      render: (v: LiveStatus) => LIVE_STATUS_MAP[v] ?? <Tag>未知</Tag>,
    },
    {
      title: '录制状态',
      dataIndex: 'record_status',
      width: 100,
      render: (v: RecordStatus) => RECORD_STATUS_MAP[v] ?? <Tag>未知</Tag>,
    },
    {
      title: '已录制',
      dataIndex: 'bytes_written',
      width: 100,
      render: (v: number) => formatBytes(v),
    },
    {
      title: '最近错误',
      dataIndex: 'last_error',
      ellipsis: true,
      render: (v: string) =>
        v ? (
          <Tooltip title={v}>
            <Text type="danger" ellipsis>
              {v}
            </Text>
          </Tooltip>
        ) : (
          <Text type="secondary">—</Text>
        ),
    },
    {
      title: '操作',
      width: 100,
      render: (_: unknown, record: Room) => (
        <Space>
          <Tooltip title="编辑">
            <Button
              type="text"
              size="small"
              icon={<EditOutlined />}
              onClick={() => openEdit(record)}
            />
          </Tooltip>
          <Popconfirm
            title={`确认删除房间 ${record.room_id}？`}
            onConfirm={() => handleDelete(record.room_id)}
            okText="删除"
            cancelText="取消"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="删除">
              <Button type="text" size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
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

      <Table
        rowKey="room_id"
        columns={columns}
        dataSource={rooms}
        loading={loading}
        pagination={false}
        size="middle"
        bordered
      />

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
          <Form.Item label="名称" name="name">
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
