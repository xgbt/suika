package recorder

import (
	"bufio"
	"bytes"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

// segmentHeaders 是分段层的头标签边界对象：维护缓存、检测序列头变化、
// 以及为新分段提供可重注入的头标签集合。
type segmentHeaders struct {
	metadata *flv.Tag // 最近一次 onMetaData 脚本标签
	videoSeq *flv.Tag // 最近一次 AVC 序列头
	audioSeq *flv.Tag // 最近一次 AAC 序列头
}

// changed 判断 tag 是否携带与缓存不同的序列头：流中途的序列头变化
// 意味着后续帧的解码配置与此前不同，应触发切段。首次见到某类序列头
// （缓存为 nil）不算变化。
func (h *segmentHeaders) changed(tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return h.videoSeq != nil && !bytes.Equal(h.videoSeq.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return h.audioSeq != nil && !bytes.Equal(h.audioSeq.Data, tag.Data)
	}
	return false
}

// absorb 把头标签（onMetaData / AVC 序列头 / AAC 序列头）存入缓存；
// 非头标签不改变缓存。
func (h *segmentHeaders) absorb(tag *flv.Tag) {
	switch {
	case tag.IsMetadata():
		h.metadata = tag
	case tag.IsAVCSequenceHeader():
		h.videoSeq = tag
	case tag.IsAACSequenceHeader():
		h.audioSeq = tag
	}
}

// forEachReinject 按固定顺序遍历可重注入头标签（metadata -> video seq -> audio seq）。
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

// segmentFile 代表一个录制分段文件，包含视频和弹幕文件，以及写入状态
type segmentFile struct {
	part      int           // 分段编号，从 1 开始
	videoPath string        // 视频文件路径
	danmuPath string        // 弹幕文件路径
	vf        *os.File      // 视频文件句柄
	df        *os.File      // 弹幕文件句柄
	bw        *bufio.Writer // 视频文件缓冲写入器
	hasStart  bool          // 是否已写入首个正文标签
	startTs   int64         // 首个正文标签的时间戳，切分时长以此为起点
	lastTs    int64         // 最近一次写入标签的时间戳
	bytes     int64         // 已写入字节数（含文件头与头标签）
	wallStart time.Time     // 分段打开的墙钟时间
}

// openSegment 打开一个新的录制分段文件，返回 segmentFile 对象。
func openSegment(dir, base string, part int, header *flv.FileHeader, headers *segmentHeaders) (*segmentFile, error) {
	videoPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.flv", base, part))
	danmuPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.danmu.jsonl", base, part))
	vf, err := os.OpenFile(videoPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	df, err := os.OpenFile(danmuPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		vf.Close()
		return nil, err
	}
	seg := &segmentFile{
		part: part, videoPath: videoPath, danmuPath: danmuPath,
		df:        df,
		vf:        vf,
		bw:        bufio.NewWriterSize(vf, 1<<20),
		wallStart: time.Now(),
	}
	// 写入 FLV 文件头及缓存的头标签（metadata/序列头），确保分段文件可独立播放。
	if err := seg.writeHeaderTags(header, headers); err != nil {
		seg.close()
		return nil, err
	}
	return seg, nil
}

// writeHeaderTags 写入 FLV 文件头与缓存的头标签（metadata、video/audio
// 序列头），使分段文件从第一帧起即可独立解码播放。
func (s *segmentFile) writeHeaderTags(header *flv.FileHeader, headers *segmentHeaders) error {
	hb := header.Bytes()
	if _, err := s.bw.Write(hb); err != nil {
		return err
	}
	s.bytes += int64(len(hb))

	var writeErr error
	headers.forEachReinject(func(tag *flv.Tag) {
		if writeErr != nil {
			return
		}
		tb := tag.AppendTo(nil)
		if _, err := s.bw.Write(tb); err != nil {
			writeErr = err
			return
		}
		s.bytes += int64(len(tb))
	})
	return writeErr
}

// writeTag 将一个 FLV 标签写入分段文件，并更新分段文件的状态。
func (s *segmentFile) writeTag(tag *flv.Tag) (int64, error) {
	buf := tag.AppendTo(nil)
	n, err := s.bw.Write(buf)
	if n > 0 {
		s.bytes += int64(n)
		if !s.hasStart {
			s.hasStart = true
			s.startTs = tag.Timestamp
		}
		s.lastTs = tag.Timestamp
	}
	return int64(n), err
}

// writeEvent 将一个弹幕事件写入分段文件，并更新分段文件的状态。
func (s *segmentFile) writeEvent(ev *biz.DanmakuEvent) error {
	line := danmuLine{
		Ts:       ev.Ts.UnixMilli(),
		SendTs:   ev.SendTs,
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
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	_, err = s.df.Write(append(data, '\n'))
	return err
}

// close 关闭分段文件，刷新缓冲区并关闭文件句柄。
func (s *segmentFile) close() error {
	err := s.bw.Flush()
	return stderrors.Join(err, s.vf.Close(), s.df.Close())
}
