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

// initTags 缓存可在分段间复用的初始化标签，用于检测序列头变化并在新分段开头重放。
type initTags struct {
	metadata       *flv.Tag // 最近一次 onMetaData 脚本标签
	videoSeqHeader *flv.Tag // 最近一次 AVC 序列头
	audioSeqHeader *flv.Tag // 最近一次 AAC 序列头
}

// seqHeaderChanged 判断 Tag 是否为与缓存不同的序列头（首次出现不算），变化时需切段。
func (it *initTags) seqHeaderChanged(tag *flv.Tag) bool {
	switch {
	case tag.IsAVCSequenceHeader():
		return it.videoSeqHeader != nil && !bytes.Equal(it.videoSeqHeader.Data, tag.Data)
	case tag.IsAACSequenceHeader():
		return it.audioSeqHeader != nil && !bytes.Equal(it.audioSeqHeader.Data, tag.Data)
	}
	// metadata 不参与判断：它只是分辨率/帧率等展示用提示值，不影响解码器配置。
	return false
}

// observe 如果 tag 是初始化标签，则更新缓存；否则不做任何操作。
func (it *initTags) observe(tag *flv.Tag) {
	switch {
	case tag.IsMetadata():
		it.metadata = tag
	case tag.IsAVCSequenceHeader():
		it.videoSeqHeader = tag
	case tag.IsAACSequenceHeader():
		it.audioSeqHeader = tag
	}
}

// segmentWriter 是一个已打开分段的句柄，持有文件与写入进度。
type segmentWriter struct {
	part int // 分段编号，从 1 开始，同时用于生成文件名

	// 视频文件
	videoPath   string        // 视频文件路径
	videoFile   *os.File      // 视频文件句柄
	videoWriter *bufio.Writer // 视频文件缓冲写入器

	// 弹幕文件（无缓冲，事件直接写入）
	danmakuPath string   // 弹幕文件路径
	danmakuFile *os.File // 弹幕文件句柄

	hasStart     bool      // 是否已写入首个正文标签；初始化标签不计，未写入时 startTs 无效
	startTs      int64     // 首个正文标签的时间戳，切分时长以此为起点
	lastTs       int64     // 最近一次写入的正文标签的时间戳，即本段结束时间
	bytesWritten int64     // 已写入视频文件的字节数，含 FLV 文件头、初始化标签与正文标签
	wallStart    time.Time // 分段打开的墙钟时间，对应 meta.json 的 WallStart
}

// close 刷新视频缓冲并关闭两个文件。任一步骤失败都不会跳过后续步骤，
// 以保证文件句柄全部释放，所有错误合并返回。
func (sw *segmentWriter) close() error {
	var errs []error

	// 视频文件需先刷新缓冲再关闭，否则尾部数据丢失；弹幕文件无缓冲，直接关闭即可。
	if err := sw.videoWriter.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("flush video buffer: %w", err))
	}
	if err := sw.videoFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close video file: %w", err))
	}
	if err := sw.danmakuFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close danmaku file: %w", err))
	}

	return errors.Join(errs...)
}

// OpenSegment 创建分段文件并写入 FLV 文件头与缓存的初始化标签；失败时清理已创建的文件。
func OpenSegment(lay sessionLayout, fileHeader *flv.FileHeader, initTags *initTags) (writer *segmentWriter, err error) {
	// 初始化分段编号和文件路径
	part := nextPartNumber(lay)
	videoPath := lay.segmentVideoPath(part)
	danmakuPath := lay.segmentDanmakuPath(part)

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

	// 打开视频和弹幕文件
	if videoFile, err = os.OpenFile(videoPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err != nil {
		return nil, err
	}
	if danmakuFile, err = os.OpenFile(danmakuPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err != nil {
		return nil, err
	}

	// 初始化分段写入器结构体，准备写入视频和弹幕数据。
	writer = &segmentWriter{
		part:        part,
		videoPath:   videoPath,
		videoFile:   videoFile,
		videoWriter: bufio.NewWriterSize(videoFile, 1<<20), // 1MB 缓冲
		danmakuPath: danmakuPath,
		danmakuFile: danmakuFile,
		wallStart:   time.Now(),
	}

	// 写入 FLV 文件头及缓存的初始化标签（metadata/序列头），确保分段文件可独立播放。
	if err = writer.writePreamble(fileHeader, initTags); err != nil {
		return nil, err
	}

	return writer, nil
}

// writePreamble 依次写入 FLV 文件头与缓存的初始化标签，即分段正文之前的全部内容。
func (sw *segmentWriter) writePreamble(fileHeader *flv.FileHeader, initTags *initTags) error {
	// 写入 FLV 文件头
	if _, err := sw.videoWriter.Write(fileHeader.Bytes()); err != nil {
		return err
	}
	sw.bytesWritten += int64(len(fileHeader.Bytes()))

	// 写入缓存的初始化标签（metadata/序列头）
	for _, tag := range []*flv.Tag{initTags.metadata, initTags.videoSeqHeader, initTags.audioSeqHeader} {
		if tag == nil {
			continue
		}
		if _, err := sw.writeRawTag(tag); err != nil {
			return err
		}
	}

	return nil
}

// writeRawTag 将标签序列化并写入视频文件的缓冲区，仅累计字节数，不推进时间戳。
func (sw *segmentWriter) writeRawTag(tag *flv.Tag) (int64, error) {
	encoded := tag.AppendTo(sw.videoWriter.AvailableBuffer())
	n, err := sw.videoWriter.Write(encoded)
	sw.bytesWritten += int64(n)
	return int64(n), err
}

// writeBodyTag 写入正文标签，仅在写入成功时推进起止时间戳。
func (sw *segmentWriter) writeBodyTag(tag *flv.Tag) (int64, error) {
	n, err := sw.writeRawTag(tag)
	if err != nil {
		return n, err
	}
	if !sw.hasStart {
		sw.hasStart = true
		sw.startTs = tag.Timestamp
	}
	sw.lastTs = tag.Timestamp
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
func (sw *segmentWriter) writeDanmaku(ev *biz.DanmakuEvent) error {
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
	return json.NewEncoder(sw.danmakuFile).Encode(entry)
}

const (
	splitOverrun            = 15 * time.Second // 分段在等待关键帧切点时最多超出目标时长
	sizeSplitOverrunDivisor = 10               // 大小切分等待关键帧的强切裕度分母
)

// 分段上限，暂不开放配置。用 var 仅为让测试缩小阈值，生产代码不得修改。
var (
	segmentSizeLimit     int64 = 2_684_354_560 // 2.5 GiB
	segmentDurationLimit       = 120 * time.Minute
)

// shouldSplit 判断下一个 tag 是否应开启新分段。两个独立触发条件，都优先
// 等待视频关键帧以保证分段可独立播放：
//  1. 大小：已写字节达到上限，且该 tag 是关键帧；或超出上限的
//     1/sizeSplitOverrunDivisor 裕度仍无关键帧则强制切分；
//  2. 时长：达到目标时长且该 tag 是关键帧；或超出 splitOverrun 强制切分。
func (sw *segmentWriter) shouldSplit(tag *flv.Tag) bool {
	if !sw.hasStart {
		return false
	}
	return sw.splitBySize(tag, segmentSizeLimit) || sw.splitByDuration(tag, segmentDurationLimit)
}

// splitBySize 判断当前分段是否应当因体积超限而切分。
//
// 切分策略：
//  1. 若未配置 maxSegmentBytes（<=0），表示不启用按大小切分，直接返回 false。
//  2. 若当前分段体积尚未达到 maxSegmentBytes，无需切分。
//  3. 一旦达到阈值，优先在关键帧处切分，以保证新分段能独立解码播放。
//  4. 若达到阈值后迟迟等不到关键帧，为避免单个分段无限膨胀，
//     允许体积在阈值基础上再超出 overrun（maxSegmentBytes/sizeSplitOverrunDivisor）后强制切分，
//     即便当前 tag 不是关键帧。
func (sw *segmentWriter) splitBySize(tag *flv.Tag, maxSegmentBytes int64) bool {
	// 未设置最大分段字节数，不按大小切分
	if maxSegmentBytes <= 0 {
		return false
	}

	// 尚未达到阈值，无需切分
	if sw.bytesWritten < maxSegmentBytes {
		return false
	}

	// 已达到阈值：关键帧处可直接切分，保证分段独立可解码
	if tag.IsAVCKeyframe() {
		return true
	}

	// 非关键帧：仅当体积超出阈值 + 容忍裕度后，才强制切分
	overrunThreshold := maxSegmentBytes + maxSegmentBytes/sizeSplitOverrunDivisor
	return sw.bytesWritten >= overrunThreshold
}

// splitByDuration 判断当前分段是否应当因时长超限而切分。
//
// 切分策略：
//  1. 若未配置 segmentDuration（<=0），表示不启用按时长切分，直接返回 false。
//  2. 根据当前 tag 时间戳与分段起始时间戳的差值计算已录制时长 elapsed，
//     若尚未达到 segmentDuration，无需切分。
//  3. 一旦达到阈值，优先在关键帧处切分，以保证新分段能独立解码播放。
//  4. 若达到阈值后迟迟等不到关键帧，允许时长在阈值基础上再超出 splitOverrun 后强制切分，
//     即便当前 tag 不是关键帧，避免单个分段无限拉长。
func (sw *segmentWriter) splitByDuration(tag *flv.Tag, segmentDuration time.Duration) bool {
	// 未设置分段时长，不按时长切分
	if segmentDuration <= 0 {
		return false
	}

	// 已录制时长 = 当前 tag 时间戳 - 分段起始时间戳
	elapsed := time.Duration(tag.Timestamp-sw.startTs) * time.Millisecond

	// 尚未达到阈值，无需切分
	if elapsed < segmentDuration {
		return false
	}

	// 已达到阈值：关键帧处可直接切分；否则仅当超出容忍裕度后强制切分
	overrunThreshold := segmentDuration + splitOverrun
	return tag.IsAVCKeyframe() || elapsed >= overrunThreshold
}
