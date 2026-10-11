package recorder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

// headerCache 缓存可在分段间复用的头标签，用于检测序列头变化并在新分段开头重放。
type headerCache struct {
	metadata       *flv.Tag // 最近一次 onMetaData 脚本标签
	videoSeqHeader *flv.Tag // 最近一次 AVC 序列头
	audioSeqHeader *flv.Tag // 最近一次 AAC 序列头
}

// sequenceHeaderChanged 报告 tag 是否为与缓存不同的序列头（首次出现不算），变化时需切段。
func (h *headerCache) sequenceHeaderChanged(tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return h.videoSeqHeader != nil && !bytes.Equal(h.videoSeqHeader.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return h.audioSeqHeader != nil && !bytes.Equal(h.audioSeqHeader.Data, tag.Data)
	}
	// metadata 不参与判断：它只是分辨率/帧率等展示用提示值，不影响解码器配置。
	return false
}

// remember 缓存头标签；非头标签不改变缓存。
func (h *headerCache) remember(tag *flv.Tag) {
	switch {
	case tag.IsMetadata():
		h.metadata = tag
	case tag.IsAVCSequenceHeader():
		h.videoSeqHeader = tag
	case tag.IsAACSequenceHeader():
		h.audioSeqHeader = tag
	}
}

// segmentWriter 是一个已打开分段的句柄，持有文件与写入进度。
type segmentWriter struct {
	part int // 分段编号，从 1 开始，同时用于生成文件名

	// 视频文件
	videoPath   string        // 视频文件路径
	videoFile   *os.File      // 视频文件句柄
	videoWriter *bufio.Writer // 视频文件缓冲写入器（1MB）

	// 弹幕文件（无缓冲，事件直接写入）
	danmakuPath string   // 弹幕文件路径
	danmakuFile *os.File // 弹幕文件句柄

	buf []byte // 标签序列化复用缓冲，writeRawTag 每次复用，避免逐标签分配

	hasStart     bool      // 是否已写入首个正文标签；头标签不计，未写入时 startTs 无效
	startTs      int64     // 首个正文标签的时间戳，切分时长以此为起点
	lastTs       int64     // 最近一次写入的正文标签的时间戳，即本段结束时间
	bytesWritten int64     // 已写入视频文件的字节数，含 FLV 文件头、头标签与正文标签
	wallStart    time.Time // 分段打开的墙钟时间，对应 meta.json 的 WallStart
}

// close 刷新视频缓冲并关闭两个文件。任一步骤失败都不会跳过后续步骤，
// 以保证文件句柄全部释放，所有错误合并返回。
func (s *segmentWriter) close() error {
	var errs []error

	// 视频文件需先刷新缓冲再关闭，否则尾部数据丢失；弹幕文件无缓冲，直接关闭即可。
	if err := s.videoWriter.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("flush video buffer: %w", err))
	}
	if err := s.videoFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close video file: %w", err))
	}
	if err := s.danmakuFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close danmaku file: %w", err))
	}

	return errors.Join(errs...)
}

// openSegment 创建分段文件并写入 FLV 文件头与缓存的头标签；失败时清理已创建的文件。
// headerTagBytes 为头标签字节数（不含文件头），供调用方计入写入进度。
func openSegment(lay sessionLayout, header *flv.FileHeader, cache *headerCache) (writer *segmentWriter, headerTagBytes int64, err error) {
	part := nextPartNumber(lay)
	videoPath := lay.segmentVideoPath(part)
	danmakuPath := lay.segmentDanmakuPath(part)

	// 依赖命名返回值 err：失败时关闭并删除已打开的文件，不留半成品分段。
	var videoFile, danmakuFile *os.File
	defer func() {
		if err == nil {
			return
		}
		if videoFile != nil {
			videoFile.Close()
			os.Remove(videoPath)
		}
		if danmakuFile != nil {
			danmakuFile.Close()
			os.Remove(danmakuPath)
		}
	}()

	if videoFile, err = os.OpenFile(videoPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err != nil {
		return nil, 0, err
	}
	// 与视频文件一致使用 O_TRUNC，part 编号复用时避免旧弹幕残留。
	if danmakuFile, err = os.OpenFile(danmakuPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err != nil {
		return nil, 0, err
	}

	writer = &segmentWriter{
		part:        part,
		videoPath:   videoPath,
		videoFile:   videoFile,
		videoWriter: bufio.NewWriterSize(videoFile, 1<<20),
		danmakuPath: danmakuPath,
		danmakuFile: danmakuFile,
		wallStart:   time.Now(),
	}

	// 写入 FLV 文件头及缓存的头标签（metadata/序列头），确保分段文件可独立播放。
	if headerTagBytes, err = writer.writeHeaderTags(header, cache); err != nil {
		return nil, 0, err
	}

	return writer, headerTagBytes, nil
}

// writeHeaderTags 写入 FLV 文件头及缓存的头标签，返回头标签字节数（不含文件头）。
func (s *segmentWriter) writeHeaderTags(header *flv.FileHeader, cache *headerCache) (int64, error) {
	// 写入 FLV 文件头
	if _, err := s.videoWriter.Write(header.Bytes()); err != nil {
		return 0, err
	}
	s.bytesWritten += int64(len(header.Bytes()))

	// 写入缓存的头标签（metadata/序列头）
	var tagBytes int64
	for _, tag := range []*flv.Tag{cache.metadata, cache.videoSeqHeader, cache.audioSeqHeader} {
		if tag == nil {
			continue
		}
		// 写入单个头标签
		n, err := s.writeRawTag(tag)
		tagBytes += n
		if err != nil {
			return tagBytes, err
		}
	}
	return tagBytes, nil
}

// writeRawTag 序列化 tag 并写入视频文件，只累计字节数，不推进时间戳。
func (s *segmentWriter) writeRawTag(tag *flv.Tag) (int64, error) {
	s.buf = tag.AppendTo(s.buf[:0])
	n, err := s.videoWriter.Write(s.buf)
	s.bytesWritten += int64(n)
	return int64(n), err
}

// writeBodyTag 写入正文标签，仅在写入成功时推进起止时间戳。
func (s *segmentWriter) writeBodyTag(tag *flv.Tag) (int64, error) {
	n, err := s.writeRawTag(tag)
	if err != nil {
		return n, err
	}
	if !s.hasStart {
		s.hasStart = true
		s.startTs = tag.Timestamp
	}
	s.lastTs = tag.Timestamp
	return n, nil
}

// danmakuEventPO 是弹幕事件的持久化对象（PO），即 biz.DanmakuEvent（DO）落盘到弹幕 JSONL
// 时的存储形状。与 DO 的差异：时间戳为 unix 毫秒整数而非 time.Time，字段带 JSON tag，
// 空值按 omitempty 省略。
type danmakuEventPO struct {
	Ts       int64           `json:"ts"`                  // 接收时刻（unix 毫秒）
	SendTs   int64           `json:"send_ts,omitempty"`   // 平台载荷中的发送时刻（unix 毫秒），未知省略
	Type     string          `json:"type"`                // 事件类型，如弹幕、礼物、进场特效等
	UID      int64           `json:"uid,omitempty"`       // 用户 ID
	Uname    string          `json:"uname,omitempty"`     // 用户昵称
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

// writeDanmaku 将 DO 转换为 danmakuEventPO 后序列化为一行 JSON 写入弹幕文件。
func (s *segmentWriter) writeDanmaku(ev *biz.DanmakuEvent) error {
	entry := danmakuEventPO{
		Ts:       ev.TS.UnixMilli(),
		SendTs:   ev.SendTS,
		Type:     ev.Type,
		UID:      ev.UID,
		Uname:    ev.Uname,
		Text:     ev.Text,
		Color:    ev.Color,
		Mode:     ev.Mode,
		GiftName: ev.GiftName,
		Num:      ev.Num,
		Price:    ev.Price,
		CoinType: ev.CoinType,
		Duration: ev.Duration,
		Level:    ev.Level,
		Raw:      ev.Raw,
	}
	return json.NewEncoder(s.danmakuFile).Encode(entry)
}
