package recorder

import (
	"time"

	"suika/internal/data/flv"
)

const (
	splitOverrun = 15 * time.Second // 分段在等待关键帧切点时最多超出目标时长
	// sizeSplitOverrunDivisor 大小切分等待关键帧的强切裕度：超出阈值
	// 1/该值仍未等到关键帧则强制切分（GOP 增量相对 GiB 级阈值可忽略）。
	sizeSplitOverrunDivisor = 10
)

// segmentSplitPolicy 负责分段判定策略：按大小和时长两个维度独立裁决。
type segmentSplitPolicy struct {
	maxSegmentBytes int64         // 分段大小上限，<= 0 时不按大小切分
	segmentDuration time.Duration // 分段时长上限，<= 0 时不按时长切分
}

// shouldSplit 判断下一个 tag 是否应开启新分段。两个独立触发条件，都优先
// 等待视频关键帧以保证分段可独立播放：
//  1. 大小：已写字节达到上限，且该 tag 是关键帧；或超出上限的
//     1/sizeSplitOverrunDivisor 裕度仍无关键帧则强制切分；
//  2. 时长：达到目标时长且该 tag 是关键帧；或超出 splitOverrun 强制切分。
func (p segmentSplitPolicy) shouldSplit(seg *segmentFile, tag *flv.Tag) bool {
	if !seg.hasStart {
		return false
	}
	if p.shouldSplitBySize(seg, tag) {
		return true
	}
	return p.shouldSplitByDuration(seg, tag)
}

func (p segmentSplitPolicy) shouldSplitBySize(seg *segmentFile, tag *flv.Tag) bool {
	if p.maxSegmentBytes <= 0 || seg.bytes < p.maxSegmentBytes {
		return false
	}
	overrun := p.maxSegmentBytes / sizeSplitOverrunDivisor
	return tag.IsVideoKeyframe() || seg.bytes >= p.maxSegmentBytes+overrun
}

func (p segmentSplitPolicy) shouldSplitByDuration(seg *segmentFile, tag *flv.Tag) bool {
	if p.segmentDuration <= 0 {
		return false
	}
	elapsed := time.Duration(tag.Timestamp-seg.startTs) * time.Millisecond
	if elapsed < p.segmentDuration {
		return false
	}
	return tag.IsVideoKeyframe() || elapsed >= p.segmentDuration+splitOverrun
}

func (r *recorderRepo) shouldSplit(seg *segmentFile, tag *flv.Tag) bool {
	return segmentSplitPolicy{
		maxSegmentBytes: r.maxSegmentBytes,
		segmentDuration: r.segmentDuration,
	}.shouldSplit(seg, tag)
}
