package recorder

import "suika/internal/data/flv"

// CDN 循环吐流去重（参照录播姬 RemoveDuplicatedChunkRule）。
const (
	dedupWindowSize       = 16       // 块指纹滑动窗口大小
	dedupMaxStreak        = 10       // 连续重复块达到该值即判定为循环吐流，请求断开
	dedupGapMs      int64 = 25_000   // 相邻 tag 时间戳间隔上限，纯音频流唯一的自然边界
	dedupSpanMs     int64 = 60_000   // 单块时长上限，防止病态长 GOP 撑爆缓冲
	dedupMaxBytes   int64 = 64 << 20 // 单块字节上限，框定内存占用

	digestSampleBytes = 64 // 单 tag 指纹仅采样载荷前 64 字节
)

// dedupGuard 检测 CDN 循环吐流。
//
// tag 按"块"缓冲（从视频关键帧起，到下一个边界止），块关闭时用内容指纹
// 与最近窗口比对：重复块整体丢弃（不落盘），连续重复达到上限则请求断开，
// 由调用方包装为 ErrStreamTransient 触发换流重连。
// 指纹不含时间戳，可容忍循环重放时时间戳被重写。
type dedupGuard struct {
	// 指纹窗口（环形）
	window [dedupWindowSize]uint64
	total  int // 累计入窗次数，可超过窗口长度
	streak int // 连续重复块计数

	// 当前块
	tags    []*flv.Tag
	size    int64  // 估算字节数（载荷 + FLV 封装开销）
	hash    uint64 // 块内 tag 指纹的顺序折叠值
	firstTS int64  // 仅用于边界判断，不参与指纹
	lastTS  int64
}

// atBoundary 判断 tag 是否应先关闭当前块。
// 头标签和元数据也视为边界，避免它们被缓冲到下一块之后落盘，打乱与数据标签的顺序。
func (g *dedupGuard) atBoundary(tag *flv.Tag) bool {
	if len(g.tags) == 0 {
		return false
	}
	switch {
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

// flush 结束当前块并重置：返回应写入的 tag（空块或重复块为 nil），
// 以及连续重复是否已达断开上限。
func (g *dedupGuard) flush() (out []*flv.Tag, disconnect bool) {
	if len(g.tags) == 0 {
		return nil, false
	}

	fp := g.fingerprint()
	dup := g.seen(fp)

	// 重复块同样入窗，保证连续计数不因登记而中断
	g.window[g.total%dedupWindowSize] = fp
	g.total++
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

// drain 取走当前块全部 tag，不做重复裁决（流结束或中止时尽量保留已收数据）。
func (g *dedupGuard) drain() []*flv.Tag {
	tags := g.tags
	g.tags, g.size, g.hash, g.firstTS, g.lastTS = nil, 0, 0, 0, 0
	return tags
}

// fingerprint 混入 tag 数以区分内容折叠值相同但长度不同的块。
func (g *dedupGuard) fingerprint() uint64 {
	return g.hash ^ uint64(len(g.tags))*0x9E3779B97F4A7C15
}

// seen 判断指纹是否命中窗口。
func (g *dedupGuard) seen(fp uint64) bool {
	for _, h := range g.window[:min(g.total, dedupWindowSize)] {
		if h == fp {
			return true
		}
	}
	return false
}

// tagDigest 计算单个 tag 的 FNV-1a 64 位指纹，输入为载荷长度、tag 类型和
// 载荷前缀。前缀采样是吞吐与误判率的折中，误判的影响由块级聚合与连续重复
// 阈值兜底；不含时间戳以抵御时间戳重写。
func tagDigest(tag *flv.Tag) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)

	h := offset
	h = (h ^ uint64(len(tag.Data))) * prime
	h = (h ^ uint64(tag.Type)) * prime

	data := tag.Data
	if len(data) > digestSampleBytes {
		data = data[:digestSampleBytes]
	}
	for _, b := range data {
		h = (h ^ uint64(b)) * prime
	}
	return h
}
