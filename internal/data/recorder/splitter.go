package recorder

import (
	"time"

	"suika/internal/data/flv"
)

const (
	defaultSegmentMinutes = 120 // 分段时长（分钟）

	// defaultMaxSegmentBytes 分段大小上限（2.5 GiB，对齐 biliup 默认值）：
	// 原画长直播的单段体积和崩溃时的损失半径由此封顶，与时长上限取或。
	defaultMaxSegmentBytes int64 = 2_684_354_560

	splitOverrun = 15 * time.Second // 分段在等待关键帧切点时最多超出目标时长
	// sizeSplitOverrunDivisor 大小切分等待关键帧的强切裕度：超出阈值
	// 1/该值仍未等到关键帧则强制切分（GOP 增量相对 GiB 级阈值可忽略）。
	sizeSplitOverrunDivisor = 10
)

// splitter 负责分段判定策略：按大小和时长两个维度独立裁决。
type splitter struct {
	maxSegmentBytes int64         // 分段大小上限，<= 0 时不按大小切分
	segmentDuration time.Duration // 分段时长上限，<= 0 时不按时长切分
}

func newSplitter() splitter {
	return splitter{
		maxSegmentBytes: defaultMaxSegmentBytes,
		segmentDuration: defaultSegmentMinutes * time.Minute,
	}
}

// shouldSplit 判断下一个 tag 是否应开启新分段。两个独立触发条件，都优先
// 等待视频关键帧以保证分段可独立播放：
//  1. 大小：已写字节达到上限，且该 tag 是关键帧；或超出上限的
//     1/sizeSplitOverrunDivisor 裕度仍无关键帧则强制切分；
//  2. 时长：达到目标时长且该 tag 是关键帧；或超出 splitOverrun 强制切分。
func (s splitter) shouldSplit(seg *recordingSegment, tag *flv.Tag) bool {
	if !seg.hasStart {
		return false
	}

	return s.byBytes(seg, tag) || s.byDuration(seg, tag)
}

// byBytes 判断当前分段是否应当因体积超限而切分。
//
// 切分策略：
//  1. 若未配置 maxSegmentBytes（<=0），表示不启用按大小切分，直接返回 false。
//  2. 若当前分段体积尚未达到 maxSegmentBytes，无需切分。
//  3. 一旦达到阈值，优先在关键帧处切分，以保证新分段能独立解码播放。
//  4. 若达到阈值后迟迟等不到关键帧，为避免单个分段无限膨胀，
//     允许体积在阈值基础上再超出 overrun（maxSegmentBytes/sizeSplitOverrunDivisor）后强制切分，
//     即便当前 tag 不是关键帧。
func (s splitter) byBytes(seg *recordingSegment, tag *flv.Tag) bool {
	// 未设置最大分段字节数，不按大小切分
	if s.maxSegmentBytes <= 0 {
		return false
	}

	// 尚未达到阈值，无需切分
	if seg.bytes < s.maxSegmentBytes {
		return false
	}

	// 已达到阈值：关键帧处可直接切分，保证分段独立可解码
	if tag.IsVideoKeyframe() {
		return true
	}

	// 非关键帧：仅当体积超出阈值 + 容忍裕度后，才强制切分
	overrunThreshold := s.maxSegmentBytes + s.maxSegmentBytes/sizeSplitOverrunDivisor
	return seg.bytes >= overrunThreshold
}

// byDuration 判断当前分段是否应当因时长超限而切分。
//
// 切分策略：
//  1. 若未配置 segmentDuration（<=0），表示不启用按时长切分，直接返回 false。
//  2. 根据当前 tag 时间戳与分段起始时间戳的差值计算已录制时长 elapsed，
//     若尚未达到 segmentDuration，无需切分。
//  3. 一旦达到阈值，优先在关键帧处切分，以保证新分段能独立解码播放。
//  4. 若达到阈值后迟迟等不到关键帧，允许时长在阈值基础上再超出 splitOverrun 后强制切分，
//     即便当前 tag 不是关键帧，避免单个分段无限拉长。
func (s splitter) byDuration(seg *recordingSegment, tag *flv.Tag) bool {
	// 未设置分段时长，不按时长切分
	if s.segmentDuration <= 0 {
		return false
	}

	// 已录制时长 = 当前 tag 时间戳 - 分段起始时间戳
	elapsed := time.Duration(tag.Timestamp-seg.startTs) * time.Millisecond

	// 尚未达到阈值，无需切分
	if elapsed < s.segmentDuration {
		return false
	}

	// 已达到阈值：关键帧处可直接切分；否则仅当超出容忍裕度后强制切分
	overrunThreshold := s.segmentDuration + splitOverrun
	return tag.IsVideoKeyframe() || elapsed >= overrunThreshold
}
