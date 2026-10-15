package recorder

import (
	"slices"

	"suika/internal/data/flv"
)

// CDN 循环吐流去重（参照录播姬 RemoveDuplicatedChunkRule）。
const (
	dedupWindowSize       = 16       // 块指纹滑动窗口大小
	dedupMaxStreak        = 10       // 连续重复块达到该值即判定为循环吐流，请求断开
	dedupGapMs      int64 = 25_000   // 相邻 tag 时间戳间隔上限，纯音频流唯一的自然边界
	dedupSpanMs     int64 = 60_000   // 单块时长上限，防止病态长 GOP 撑爆缓冲
	dedupMaxBytes   int64 = 64 << 20 // 单块字节上限，框定内存占用

	digestSampleBytes = 64 // 单 tag 指纹仅采样载荷前 64 字节
)

// dedupGuard 按块缓冲 tag 并检测 CDN 循环吐流。
type dedupGuard struct {
	hits   [dedupWindowSize]uint64 // 最近块的指纹窗口（环形），块关闭时与之比对
	hitsN  int                     // 累计入窗次数，可超过窗口长度
	streak int                     // 连续重复块计数，达到上限由调用方包装为 ErrStreamTransient 触发换流重连

	tags []*flv.Tag // 当前块的 tag 列表

	size    int64  // 估算字节数（载荷 + FLV 封装开销）
	hash    uint64 // 块内 tag 指纹的顺序折叠值，不含时间戳，可容忍循环重放时时间戳被重写
	firstTS int64  // 仅用于边界判断，不参与指纹
	lastTS  int64  // 当前块最后一个 tag 的时间戳
}

// atBoundary 判断 tag 是否应先关闭当前块。
func (g *dedupGuard) atBoundary(tag *flv.Tag) bool {
	if len(g.tags) == 0 {
		return false
	}
	switch {
	// 头标签和元数据也作边界，避免它们被缓冲到下一块之后落盘，打乱与数据标签的顺序
	case tag.IsAVCKeyframe(), tag.IsMetadata(),
		tag.IsAVCSequenceHeader(), tag.IsAACSequenceHeader():
		return true
	case tag.Timestamp-g.lastTS >= dedupGapMs:
		return true
	case tag.Timestamp-g.firstTS >= dedupSpanMs:
		return true
	}
	return g.size >= dedupMaxBytes
}

// push 将 tag 折入当前块，只缓冲不落盘。
func (g *dedupGuard) push(tag *flv.Tag) {
	if len(g.tags) == 0 {
		g.firstTS = tag.Timestamp
	}
	g.lastTS = tag.Timestamp
	g.hash = g.hash*131 + tagDigest(tag)
	g.size += int64(len(tag.Data)) + flv.TagOverhead
	g.tags = append(g.tags, tag)
}

// flush 结束当前块并重置，返回应写入的 tag（空块或重复块为 nil）及是否需断开。
func (g *dedupGuard) flush() (out []*flv.Tag, disconnect bool) {
	if len(g.tags) == 0 {
		return nil, false
	}

	fp := g.fingerprint()
	dup := g.seen(fp)

	// 重复块同样入窗，保证连续计数不因登记而中断
	g.hits[g.hitsN%dedupWindowSize] = fp
	g.hitsN++
	if dup {
		g.streak++
	} else {
		g.streak = 0
	}

	tags := g.drain()
	if dup {
		return nil, g.streak >= dedupMaxStreak
	}
	return tags, false
}

// drain 取走当前块全部 tag 且不判重，流结束或中止时用以保留已收数据。
func (g *dedupGuard) drain() []*flv.Tag {
	tags := g.tags
	g.tags, g.size, g.hash, g.firstTS, g.lastTS = nil, 0, 0, 0, 0
	return tags
}

func (g *dedupGuard) fingerprint() uint64 {
	// 混入 tag 数以区分折叠值相同但长度不同的块
	return g.hash ^ uint64(len(g.tags))*0x9E3779B97F4A7C15
}

// seen 判断指纹是否命中窗口。
func (g *dedupGuard) seen(fp uint64) bool {
	return slices.Contains(g.hits[:min(g.hitsN, dedupWindowSize)], fp)
}

// tagDigest 计算单个 tag 的 FNV-1a 64 位指纹。
func tagDigest(tag *flv.Tag) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)

	// 不含时间戳，以抵御时间戳重写
	h := offset
	h = (h ^ uint64(len(tag.Data))) * prime
	h = (h ^ uint64(tag.Type)) * prime

	// 前缀采样是吞吐与误判率的折中，误判由块级聚合与连续重复阈值兜底
	data := tag.Data
	if len(data) > digestSampleBytes {
		data = data[:digestSampleBytes]
	}
	for _, b := range data {
		h = (h ^ uint64(b)) * prime
	}
	return h
}
