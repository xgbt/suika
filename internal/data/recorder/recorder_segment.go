package recorder

import (
	"bufio"
	"bytes"
	"encoding/json"
	stderrors "errors"
	"os"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

// segmentHeaders 缓存分段间可复用的头标签（onMetaData / AVC 序列头 / AAC 序列头），
// 用于检测序列头变化，并在开新分段时重新注入，使每个分段都能独立解码播放。
type segmentHeaders struct {
	metadata *flv.Tag // 最近一次 onMetaData 脚本标签
	videoSeq *flv.Tag // 最近一次 AVC 序列头
	audioSeq *flv.Tag // 最近一次 AAC 序列头
}

// sequenceHeaderChanged 判断 tag 是否携带与缓存不同的序列头：流中途的序列头变化
// 意味着后续帧的解码配置与此前不同，应触发切段。首次见到某类序列头
// （缓存为 nil）不算变化。
func (h *segmentHeaders) sequenceHeaderChanged(tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return h.videoSeq != nil && !bytes.Equal(h.videoSeq.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return h.audioSeq != nil && !bytes.Equal(h.audioSeq.Data, tag.Data)
	}
	// metadata 变化不触发切段：它只影响播放器展示的元信息（分辨率/帧率等提示值），不影响解码器配置。
	return false
}

// observe 把头标签（onMetaData / AVC 序列头 / AAC 序列头）存入缓存；
// 非头标签不改变缓存。
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

// recordingSegment 表示一个录制分段（一段视频文件 + 对应弹幕文件）及其写入状态。
type recordingSegment struct {
	part        int           // 分段编号，从 1 开始
	videoPath   string        // 视频文件路径
	danmakuPath string        // 弹幕文件路径
	videoFile   *os.File      // 视频文件句柄
	danmakuFile *os.File      // 弹幕文件句柄
	videoWriter *bufio.Writer // 视频文件缓冲写入器
	hasStart    bool          // 是否已写入首个正文标签
	startTs     int64         // 首个正文标签的时间戳，切分时长以此为起点
	lastTs      int64         // 最近一次写入标签的时间戳
	bytes       int64         // 已写入字节数（含文件头与头标签）
	wallStart   time.Time     // 分段打开的墙钟时间
}

// openSegment 创建并打开一个新的录制分段，写入 FLV 文件头及缓存的头标签后返回；
// headerTagBytes 是头标签本身占用的字节数（不含 FLV 文件头），供调用方计入写入进度。
// 任一步骤失败时，已创建的文件句柄和磁盘文件会被自动清理。
func openSegment(lay sessionLayout, part int, header *flv.FileHeader, headers *segmentHeaders) (seg *recordingSegment, headerTagBytes int64, err error) {
	videoPath := lay.segmentVideoPath(part)
	danmakuPath := lay.segmentDanmakuPath(part)

	// videoFile/danmakuFile 是仅在本函数内赋值的局部变量，defer 借助命名返回值 err
	// 判断本次调用是否失败：一旦失败，已打开到此刻的句柄和文件都会被回滚清理，
	// 不会把半成品分段遗留在磁盘上等下次复用同一 part 时才暴露问题。
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
	// danmaku 与 video 保持一致的 O_TRUNC：part 编号被复用时两个文件都重写，
	// 避免旧弹幕内容残留导致与新分段的时间轴错位、内容重复。
	if danmakuFile, err = os.OpenFile(danmakuPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err != nil {
		return nil, 0, err
	}

	seg = &recordingSegment{
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

// writeHeaderTags 写入 FLV 文件头与缓存的头标签（metadata、video/audio
// 序列头），使分段文件从第一帧起即可独立解码播放；返回头标签本身占用的
// 字节数（不含 FLV 文件头），供调用方计入写入进度。
func (s *recordingSegment) writeHeaderTags(header *flv.FileHeader, headers *segmentHeaders) (int64, error) {
	// 写入 FLV 文件头
	headerBytes := header.Bytes()
	if _, err := s.videoWriter.Write(headerBytes); err != nil {
		return 0, err
	}
	s.bytes += int64(len(headerBytes))

	// 写入缓存的头标签（metadata、video/audio 序列头）
	var tagBytes int64
	var writeErr error
	headers.forEachReinject(func(tag *flv.Tag) {
		if writeErr != nil {
			return
		}
		buf := tag.AppendTo(nil)
		if _, err := s.videoWriter.Write(buf); err != nil {
			writeErr = err
			return
		}
		s.bytes += int64(len(buf))
		tagBytes += int64(len(buf))
	})
	return tagBytes, writeErr
}

// forEachReinject 按固定顺序（metadata -> video seq -> audio seq）重放缓存的头标签。
func (h *segmentHeaders) forEachReinject(fn func(*flv.Tag)) {
	if h == nil {
		return
	}
	for _, tag := range []*flv.Tag{h.metadata, h.videoSeq, h.audioSeq} {
		if tag == nil {
			continue
		}
		fn(tag)
	}
}

// writeTag 将一个 FLV 标签写入分段文件，并更新分段的写入状态（起止时间戳、字节数）。
func (s *recordingSegment) writeTag(tag *flv.Tag) (int64, error) {
	buf := tag.AppendTo(nil)
	n, err := s.videoWriter.Write(buf)
	s.bytes += int64(n)
	// 只在完整写入成功时更新起止时间戳，避免部分写入（截断）被当作标签已成功写入。
	if err == nil {
		if !s.hasStart {
			s.hasStart = true
			s.startTs = tag.Timestamp
		}
		s.lastTs = tag.Timestamp
	}
	return int64(n), err
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

// writeEvent 将一个弹幕事件序列化为一行 JSON 写入弹幕文件。
func (s *recordingSegment) writeEvent(ev *biz.DanmakuEvent) error {
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
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = s.danmakuFile.Write(append(data, '\n'))
	return err
}

// close 关闭分段文件，刷新缓冲区并关闭文件句柄。
func (s *recordingSegment) close() error {
	err := s.videoWriter.Flush()
	return stderrors.Join(err, s.videoFile.Close(), s.danmakuFile.Close())
}
