package data

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

// headerCache 缓存了录制分段文件的头标签，避免每次切分分段时都重新生成。
type headerCache struct {
	metadata *flv.Tag
	videoSeq *flv.Tag
	audioSeq *flv.Tag
}

// headerChanged 判断 tag 是否携带与缓存不同的序列头：流中途的序列头变
// 化（CDN 换源、主播改码率）意味着后续帧的解码配置与此前不同，继续写
// 入旧分段会把两种配置拼进同一个文件。此时应强制切段；首次见到某类序列
// 头（缓存为 nil）不算变化。
func headerChanged(cache *headerCache, tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return cache.videoSeq != nil && !bytes.Equal(cache.videoSeq.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return cache.audioSeq != nil && !bytes.Equal(cache.audioSeq.Data, tag.Data)
	}
	return false
}

// segmentFile 代表一个录制分段文件，包含视频和弹幕文件，以及写入状态
type segmentFile struct {
	part      int           // 分段编号，从 1 开始
	videoPath string        // 视频文件路径
	danmuPath string        // 弹幕文件路径
	vf        *os.File      // 视频文件句柄
	df        *os.File      // 弹幕文件句柄
	bw        *bufio.Writer // 视频文件缓冲写入器
	hasStart  bool
	startTs   int64
	lastTs    int64
	bytes     int64
	wallStart time.Time
}

// openSegment 打开一个新的录制分段文件，返回 segmentFile 对象。
func openSegment(dir, base string, part int, header *flv.FileHeader, cache *headerCache) (*segmentFile, error) {
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
	// 写入 FLV 文件头
	hb := header.Bytes()
	if _, err := seg.bw.Write(hb); err != nil {
		seg.close()
		return nil, err
	}
	seg.bytes += int64(len(hb))
	// 写入缓存的头标签
	// 这些标签包括 metadata、video sequence header 和 audio sequence header，确保新分段文件可以独立播放。
	for _, tag := range []*flv.Tag{cache.metadata, cache.videoSeq, cache.audioSeq} {
		if tag == nil {
			continue
		}
		tb := tag.AppendTo(nil)
		if _, err := seg.bw.Write(tb); err != nil {
			seg.close()
			return nil, err
		}
		seg.bytes += int64(len(tb))
	}
	return seg, nil
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
