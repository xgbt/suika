// meta.go 包含与录制会话元数据（meta.json）相关的结构体和操作函数
package recorder

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v3/log"

	"suika/internal/utils"
)

const (
	metaStatusRecording = "recording" // 录制中，可能还会有新分段
	metaStatusMerging   = "merging"   // 录制完成，正在合并分段
	metaStatusDone      = "done"      // 录制完成，收尾完成
	metaStatusPartial   = "partial"   // 录制完成，合并失败，源分段保留待重试
)

// sessionMeta 存储在 meta.json 中, 记录 RecordingSession 录制会话的状态、分段信息、错误日志等
type sessionMeta struct {
	RoomID        int64         `json:"room_id"`                  // 房间 ID
	RoomName      string        `json:"room_name"`                // 主播名称（写入时的快照）
	Title         string        `json:"title"`                    // 直播标题（写入时的快照）
	LiveStartTime int64         `json:"live_start_time"`          // 开播时间（unix 秒）
	EndTime       int64         `json:"end_time"`                 // 收尾时间（unix 秒），录制中为 0
	Quality       qualityMeta   `json:"quality"`                  // 录制清晰度
	Status        string        `json:"status"`                   // 会话状态，取值见 metaStatus* 常量
	Segments      []segmentMeta `json:"segments"`                 // 已录制的分段列表
	MergedVideo   string        `json:"merged_video,omitempty"`   // 合并后的视频文件名，尚未合并或合并失败时为空
	MergedDanmaku string        `json:"merged_danmaku,omitempty"` // 合并后的弹幕文件名，尚未合并或合并失败时为空
	Errors        []errorMeta   `json:"errors"`                   // 录制/合并过程中发生的错误
	UpdatedAt     int64         `json:"updated_at"`               // 最近一次保存时间（unix 秒），saveMeta 自动填充
}

// qualityMeta 记录录制的清晰度信息，存储在 meta.json 中
type qualityMeta struct {
	Qn   int32  `json:"qn"`
	Desc string `json:"desc"`
}

// segmentMeta 记录每个分段的元数据，存储在 meta.json 中。源文件是否还在
// 磁盘上一律以文件系统为准（allSegmentSourcesExist），不在此另记一份。
type segmentMeta struct {
	Part      int    `json:"part"`       // 分段编号
	Video     string `json:"video"`      // 视频文件名
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

// loadMeta 读取并解析 meta.json。
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

// saveMeta 原子替换 meta.json（写临时文件 → fsync → rename），并回填 meta.UpdatedAt。
func saveMeta(path string, meta *sessionMeta) error {
	meta.UpdatedAt = time.Now().Unix()
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return utils.WriteFileAtomic(path, func(bw *bufio.Writer) (int64, error) {
		n, err := bw.Write(data)
		return int64(n), err
	})
}

// updateMeta 持锁读改写 meta.json，失败只记日志。
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

// persistMeta 持锁用整份 meta 覆盖 meta.json，会盖掉 updateMeta 的写入；错误向上返回。
func (r *recorderRepo) persistMeta(metaPath string, meta *sessionMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return saveMeta(metaPath, meta)
}

// appendSegmentMeta 追加一条分段记录到 meta.json。
func (r *recorderRepo) appendSegmentMeta(metaPath string, seg *recordingSegment) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Segments = append(meta.Segments, segmentMeta{
			Part:      seg.part,
			Video:     filepath.Base(seg.videoPath),
			Danmaku:   filepath.Base(seg.danmakuPath),
			WallStart: seg.wallStart.Unix(),
		})
	})
}

// finishSegmentMeta 回填指定分段的收尾字段（结束时间、时间戳、字节数）。
func (r *recorderRepo) finishSegmentMeta(metaPath string, seg *recordingSegment) {
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

// appendMetaError 追加一条错误记录到 meta.json。
func (r *recorderRepo) appendMetaError(metaPath, stage string, err error) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Errors = append(meta.Errors, errorMeta{Time: time.Now().Unix(), Stage: stage, Msg: err.Error()})
	})
}
