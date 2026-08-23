package data

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	metaStatusRecording = "recording" // 录制中，可能还会有新分段
	metaStatusRemuxing  = "remuxing"  // 录制完成，正在转封装
	metaStatusDone      = "done"      // 录制完成，转封装完成
	metaStatusPartial   = "partial"   // 录制完成，转封装失败，但至少有一个分段转封装成功
)

const (
	remuxStatusPending = "pending" // 转封装尚未开始
	remuxStatusOK      = "ok"      // 转封装成功
	remuxStatusFailed  = "failed"  // 转封装失败
)

// sessionMeta 关键元数据，存储在 meta.json 中, 记录录制会话的状态、分段信息、错误日志等
type sessionMeta struct {
	RoomID        int64         `json:"room_id"`
	RoomName      string        `json:"room_name"`
	Title         string        `json:"title"`
	LiveStartTime int64         `json:"live_start_time"`
	EndTime       int64         `json:"end_time"`
	Quality       qualityMeta   `json:"quality"`
	Status        string        `json:"status"`
	Segments      []segmentMeta `json:"segments"`
	Errors        []errorMeta   `json:"errors"`
	UpdatedAt     int64         `json:"updated_at"`
}

// qualityMeta 记录录制的清晰度信息，存储在 meta.json 中
type qualityMeta struct {
	Qn   int32  `json:"qn"`
	Desc string `json:"desc"`
}

// segmentMeta 记录每个分段的元数据，存储在 meta.json 中
type segmentMeta struct {
	Part        int    `json:"part"`     // 分段编号
	Video       string `json:"video"`    // 视频文件名
	FLVKept     bool   `json:"flv_kept"` // 标记 FLV 文件是否保留
	Danmaku     string `json:"danmaku"`
	WallStart   int64  `json:"wall_start"`
	WallEnd     int64  `json:"wall_end"`
	TsStart     int64  `json:"ts_start"`
	TsEnd       int64  `json:"ts_end"`
	Bytes       int64  `json:"bytes"`                 // 分段文件大小
	RemuxStatus string `json:"remux_status"`          // 转封装状态：pending, ok, failed
	RemuxError  string `json:"remux_error,omitempty"` // 转封装错误信息，仅在 RemuxStatus 为 failed 时存在
}

type errorMeta struct {
	Time  int64  `json:"time"`
	Stage string `json:"stage"`
	Msg   string `json:"msg"`
}

type danmuLine struct {
	Ts       int64           `json:"ts"`
	Type     string          `json:"type"`
	UID      int64           `json:"uid,omitempty"`
	Uname    string          `json:"uname,omitempty"`
	Text     string          `json:"text,omitempty"`
	Color    int32           `json:"color,omitempty"`
	Mode     int32           `json:"mode,omitempty"`
	GiftName string          `json:"gift_name,omitempty"`
	Num      int32           `json:"num,omitempty"`
	Price    int64           `json:"price,omitempty"`
	CoinType string          `json:"coin_type,omitempty"`
	Duration int32           `json:"duration,omitempty"`
	Level    int32           `json:"level,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
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
			Part:        seg.part,
			Video:       filepath.Base(seg.videoPath),
			Danmaku:     filepath.Base(seg.danmuPath),
			WallStart:   seg.wallStart.Unix(),
			RemuxStatus: remuxStatusPending,
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

// hasRetryableSegments 检查 meta.json 中是否有可重试的分段
func hasRetryableSegments(meta *sessionMeta) bool {
	for _, seg := range meta.Segments {
		if seg.RemuxStatus == remuxStatusFailed && seg.FLVKept {
			return true
		}
	}
	return false
}
