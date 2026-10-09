package recorder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

// segmentHeaders 缓存可在分段间复用的头标签，用于检测序列头变化并在新分段开头重放。
type segmentHeaders struct {
	metadata *flv.Tag // 最近一次 onMetaData 脚本标签
	videoSeq *flv.Tag // 最近一次 AVC 序列头
	audioSeq *flv.Tag // 最近一次 AAC 序列头
}

// sequenceHeaderChanged 报告 tag 是否为与缓存不同的序列头（首次出现不算），变化时需切段。
func (h *segmentHeaders) sequenceHeaderChanged(tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return h.videoSeq != nil && !bytes.Equal(h.videoSeq.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return h.audioSeq != nil && !bytes.Equal(h.audioSeq.Data, tag.Data)
	}
	// metadata 不参与判断：它只是分辨率/帧率等展示用提示值，不影响解码器配置。
	return false
}

// observe 缓存头标签；非头标签不改变缓存。
func (h *segmentHeaders) observe(tag *flv.Tag) {
	switch {
	case tag.IsMetadata():
		h.metadata = tag
	case tag.IsAVCSequenceHeader():
		h.videoSeq = tag
	case tag.IsAACSequenceHeader():
		h.audioSeq = tag
	}
}

// segmentWriter 是一个已打开分段的句柄，持有文件与写入进度。
type segmentWriter struct {
	part        int    // 分段编号，从 1 开始
	videoPath   string // 视频文件路径
	danmakuPath string // 弹幕文件路径

	videoFile   *os.File      // 视频文件句柄
	danmakuFile *os.File      // 弹幕文件句柄
	videoWriter *bufio.Writer // 视频文件缓冲写入器
	buf         []byte        // 标签序列化复用缓冲

	hasStart  bool      // 是否已写入首个正文标签
	startTs   int64     // 首个正文标签的时间戳，切分时长以此为起点
	lastTs    int64     // 最近一次写入标签的时间戳
	bytes     int64     // 已写入字节数（含文件头与头标签）
	wallStart time.Time // 分段打开的墙钟时间
}

// openSegment 创建分段文件并写入 FLV 文件头与缓存的头标签；失败时清理已创建的文件。
// headerTagBytes 为头标签字节数（不含文件头），供调用方计入写入进度。
func openSegment(lay sessionLayout, part int, header *flv.FileHeader, headers *segmentHeaders) (seg *segmentWriter, headerTagBytes int64, err error) {
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

	seg = &segmentWriter{
		part:        part,
		videoPath:   videoPath,
		danmakuPath: danmakuPath,
		videoFile:   videoFile,
		danmakuFile: danmakuFile,
		videoWriter: bufio.NewWriterSize(videoFile, 1<<20),
		wallStart:   time.Now(),
	}
	// 写入 FLV 文件头及缓存的头标签（metadata/序列头），确保分段文件可独立播放。
	if headerTagBytes, err = seg.writeHeaderTags(header, headers); err != nil {
		return nil, 0, err
	}
	return seg, headerTagBytes, nil
}

// writeHeaderTags 写入 FLV 文件头及缓存的头标签，返回头标签字节数（不含文件头）。
func (s *segmentWriter) writeHeaderTags(header *flv.FileHeader, headers *segmentHeaders) (int64, error) {
	headerBytes := header.Bytes()
	if _, err := s.videoWriter.Write(headerBytes); err != nil {
		return 0, err
	}
	s.bytes += int64(len(headerBytes))

	var tagBytes int64
	for _, tag := range []*flv.Tag{headers.metadata, headers.videoSeq, headers.audioSeq} {
		if tag == nil {
			continue
		}
		n, err := s.writeTagBytes(tag)
		tagBytes += n
		if err != nil {
			return tagBytes, err
		}
	}
	return tagBytes, nil
}

// writeTagBytes 序列化 tag 并写入视频文件，只累计字节数。
func (s *segmentWriter) writeTagBytes(tag *flv.Tag) (int64, error) {
	s.buf = tag.AppendTo(s.buf[:0])
	n, err := s.videoWriter.Write(s.buf)
	s.bytes += int64(n)
	return int64(n), err
}

// writeBodyTag 写入正文标签，仅在写入成功时推进起止时间戳。
func (s *segmentWriter) writeBodyTag(tag *flv.Tag) (int64, error) {
	n, err := s.writeTagBytes(tag)
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

// danmakuLine 是 biz.DanmakuEvent 落盘到弹幕 JSONL 的行结构，字段含义与
// DanmakuEvent 一致。
type danmakuLine struct {
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

// writeDanmaku 将一个弹幕事件序列化为一行 JSON 写入弹幕文件。
func (s *segmentWriter) writeDanmaku(ev *biz.DanmakuEvent) error {
	entry := danmakuLine{
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

// close 刷新视频缓冲并关闭两个文件。
func (s *segmentWriter) close() error {
	err := s.videoWriter.Flush()
	return errors.Join(err, s.videoFile.Close(), s.danmakuFile.Close())
}
