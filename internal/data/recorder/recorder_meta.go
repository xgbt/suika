package recorder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	metaStatusRecording = "recording" // 录制中，可能还会有新分段
	metaStatusMerging   = "merging"   // 录制完成，正在合并分段
	metaStatusDone      = "done"      // 录制完成，收尾完成
	metaStatusPartial   = "partial"   // 录制完成，合并失败，源分段保留待重试
)

// sessionMeta 关键元数据，存储在 meta.json 中, 记录录制会话的状态、分段信息、错误日志等
type sessionMeta struct {
	RoomID        int64         `json:"room_id"`         // 房间 ID
	RoomName      string        `json:"room_name"`       // 主播名称（写入时的快照）
	Title         string        `json:"title"`           // 直播标题（写入时的快照）
	LiveStartTime int64         `json:"live_start_time"` // 开播时间（unix 秒）
	EndTime       int64         `json:"end_time"`        // 收尾时间（unix 秒），录制中为 0
	Quality       qualityMeta   `json:"quality"`         // 录制清晰度
	Status        string        `json:"status"`          // 会话状态，取值见 metaStatus* 常量
	Segments      []segmentMeta `json:"segments"`        // 已录制的分段列表
	// MergedVideo / MergedDanmaku 是收尾合并产物的文件名；合并禁用、
	// 尚未合并或合并失败时为空。
	MergedVideo   string      `json:"merged_video,omitempty"`
	MergedDanmaku string      `json:"merged_danmaku,omitempty"`
	Errors        []errorMeta `json:"errors"`     // 录制/合并过程中发生的错误
	UpdatedAt     int64       `json:"updated_at"` // 最近一次保存时间（unix 秒），saveMeta 自动填充
}

// qualityMeta 记录录制的清晰度信息，存储在 meta.json 中
type qualityMeta struct {
	Qn   int32  `json:"qn"`
	Desc string `json:"desc"`
}

// segmentMeta 记录每个分段的元数据，存储在 meta.json 中
type segmentMeta struct {
	Part      int    `json:"part"`       // 分段编号
	Video     string `json:"video"`      // 视频文件名
	FLVKept   bool   `json:"flv_kept"`   // 标记 FLV 文件是否保留
	Danmaku   string `json:"danmaku"`    // 弹幕 JSONL 文件名，可能为空
	WallStart int64  `json:"wall_start"` // 分段打开的墙钟时间（unix 秒）
	WallEnd   int64  `json:"wall_end"`   // 分段关闭的墙钟时间（unix 秒）
	TsStart   int64  `json:"ts_start"`   // 分段内首个标签的流内时间戳（毫秒）
	TsEnd     int64  `json:"ts_end"`     // 分段内最后一个标签的流内时间戳（毫秒）
	Bytes     int64  `json:"bytes"`      // 分段文件大小
}

// errorMeta 记录一次录制/合并过程中的错误，存储在 meta.json 中。
type errorMeta struct {
	Time  int64  `json:"time"`  // 发生时间（unix 秒）
	Stage string `json:"stage"` // 发生阶段，如 record / merge
	Msg   string `json:"msg"`   // 错误信息
}

// danmuLine 是 biz.DanmakuEvent 落盘到弹幕 JSONL 的行结构，字段含义与
// DanmakuEvent 一致。
type danmuLine struct {
	Ts       int64           `json:"ts"`                // 接收时刻（unix 毫秒）
	SendTs   int64           `json:"send_ts,omitempty"` // 平台载荷中的发送时刻（unix 毫秒），未知省略
	Type     string          `json:"type"`
	UID      int64           `json:"uid,omitempty"`
	Uname    string          `json:"uname,omitempty"`
	Text     string          `json:"text,omitempty"`      // 弹幕文本 / SC 文本 / 进场特效文本
	Color    int32           `json:"color,omitempty"`     // 弹幕颜色 / SC 颜色
	Mode     int32           `json:"mode,omitempty"`      // 弹幕模式 / SC 模式
	GiftName string          `json:"gift_name,omitempty"` // 礼物名称
	Num      int32           `json:"num,omitempty"`       // 礼物/舰长数量
	Price    int64           `json:"price,omitempty"`     // 礼物价格（金瓜子）/ SC 价格
	CoinType string          `json:"coin_type,omitempty"` // 礼物类型：gold/silver
	Duration int32           `json:"duration,omitempty"`  // SC 保留秒数
	Level    int32           `json:"level,omitempty"`     // 舰长等级
	Raw      json.RawMessage `json:"raw,omitempty"`       // 原始 JSON Payload
}

// loadMeta 读取 meta 文件
func loadMeta(path string) (*sessionMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta sessionMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// saveMeta 保存 meta 文件，使用原子写入方式，避免写入中断导致文件损坏。
func saveMeta(path string, meta *sessionMeta) error {
	meta.UpdatedAt = time.Now().Unix()
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// updateMeta 加载 meta.json，执行修改函数 fn，然后保存回 meta.json
func (r *recorderRepo) updateMeta(metaPath string, fn func(*sessionMeta)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	meta, err := loadMeta(metaPath)
	if err != nil {
		return
	}
	fn(meta)
	if err := saveMeta(metaPath, meta); err != nil {
		log.Error("save meta failed", "path", metaPath, "err", err)
	}
}

// persistMeta 保存 meta.json，使用原子写入方式，避免写入中断导致文件损坏。
func (r *recorderRepo) persistMeta(metaPath string, meta *sessionMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return saveMeta(metaPath, meta)
}

// appendSegmentMeta 将新分段信息追加到 meta.json
func (r *recorderRepo) appendSegmentMeta(metaPath string, seg *segmentFile) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Segments = append(meta.Segments, segmentMeta{
			Part:      seg.part,
			Video:     filepath.Base(seg.videoPath),
			Danmaku:   filepath.Base(seg.danmuPath),
			WallStart: seg.wallStart.Unix(),
		})
	})
}

// finishSegmentMeta 更新 meta.json 中指定分段的写入状态
func (r *recorderRepo) finishSegmentMeta(metaPath string, seg *segmentFile) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		for i := range meta.Segments {
			s := &meta.Segments[i]
			if s.Part != seg.part {
				continue
			}
			s.WallEnd = time.Now().Unix()
			s.TsStart = seg.startTs
			s.TsEnd = seg.lastTs
			s.Bytes = seg.bytes
		}
	})
}

// appendMetaError 将错误信息追加到 meta.json 中
func (r *recorderRepo) appendMetaError(metaPath, stage string, err error) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Errors = append(meta.Errors, errorMeta{Time: time.Now().Unix(), Stage: stage, Msg: err.Error()})
	})
}
