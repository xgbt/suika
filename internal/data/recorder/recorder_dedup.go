package recorder

import (
	"suika/internal/data/flv"
)

// CDN 循环吐流去重常量。参照录播姬 RemoveDuplicatedChunkRule。
const (
	// dupHistorySize 块指纹滑动窗口大小。
	dupHistorySize = 16
	// dupDisconnectStreak 连续重复块达到该值判定为 CDN 边缘节点循环吐
	// 流，中止泵送并由断流决策树换流地址重连。
	dupDisconnectStreak = 10

	// dupGapThreshold（毫秒）：相邻标签时间戳间隔超过该值则关闭当前块。
	// 纯音频流没有视频关键帧，这是唯一的自然边界。
	dupGapThreshold int64 = 25_000
	// dupBlockMaxSpan（毫秒）：单块的流内时长跨度上限，超限强制关闭，
	// 防止病态长 GOP 让缓冲无限增长。
	dupBlockMaxSpan int64 = 60_000

	// dupBlockMaxBytes 单块缓冲字节上限，超限强制关闭，框定去重内存占用。
	dupBlockMaxBytes int64 = 64 << 20

	// tagDigestSampleBytes 单 tag 指纹采样字节数：仅取前 64 字节，配合
	// 载荷长度降低误判，同时避免全量遍历大包体。
	tagDigestSampleBytes = 64
)

// dupGuard 检测 CDN 循环吐流：泵送把标签按"块"（从视频关键帧开始、到
// 下一个边界之前）缓冲，块关闭时以内容指纹与最近窗口比对，重复块整块
// 丢弃，连续重复达到上限则请求断开（调用方包装为 ErrStreamTransient，
// 由断流决策树换流地址重连）。块先整块缓冲再裁决落盘，因此重复内容不
// 会写盘。指纹只含标签类型与载荷（不含时间戳），容忍循环重放时时间戳
// 被重写。
//
// 注意：当前实现使用 tag 载荷前缀采样（见 digestTag），是吞吐与误判率
// 的折中；通过块级聚合和连续重复阈值降低静态画面误判带来的影响。
type dupGuard struct {
	// hist 是最近块的指纹滑动窗口。
	hist [dupHistorySize]uint64
	// histN 是累计入窗次数（可超过窗口长度），用于定位环形写入位置。
	histN int
	// streak 是连续重复块的计数。
	streak int

	// 当前块的累积。
	buf []*flv.Tag
	// bufBytes 是当前块估算字节数（tag.Data + FLV 封装开销）。
	bufBytes int64
	// sum 是块内 tag 指纹的顺序折叠值。
	sum uint64
	// count 是块内 tag 数，用于 fingerprint 二次混入避免长度碰撞。
	count int
	// firstTs/lastTs 用于 gap/span 边界判断，不参与内容指纹。
	firstTs int64
	lastTs  int64
}

// boundary 判断 tag 是否为块边界：当前块非空且 tag 是视频关键帧、头标
// 签，或触发间隔/跨度/字节上限时，应先关闭当前块。头标签作为边界是为
// 了避免它们被缓冲到下一块之后才落盘，破坏与数据标签的先后顺序。
func (g *dupGuard) boundary(tag *flv.Tag) bool {
	if len(g.buf) == 0 {
		return false
	}
	if tag.IsVideoKeyframe() || tag.IsMetadata() ||
		tag.IsAVCSequenceHeader() || tag.IsAACSequenceHeader() {
		return true
	}
	if tag.Timestamp-g.lastTs >= dupGapThreshold {
		return true
	}
	if tag.Timestamp-g.firstTs >= dupBlockMaxSpan {
		return true
	}
	return g.bufBytes >= dupBlockMaxBytes
}

// add 把 tag 折入当前块（只缓冲，不落盘）。
func (g *dupGuard) add(tag *flv.Tag) {
	if len(g.buf) == 0 {
		g.firstTs = tag.Timestamp
	}
	g.lastTs = tag.Timestamp
	g.count++
	g.sum = g.sum*131 + digestTag(tag)
	g.bufBytes += int64(len(tag.Data)) + flv.TagEnvelopeSize
	g.buf = append(g.buf, tag)
}

// close 结束当前块并清空累积：返回应写入的标签（重复块与空块为
// nil），以及连续重复是否已达断开上限。重复块同样计入指纹窗口，连续
// 计数不因窗口登记而中断。
func (g *dupGuard) close() (write []*flv.Tag, disconnect bool) {
	if len(g.buf) == 0 {
		return nil, false
	}
	fp := g.fingerprint()
	dup := g.seen(fp)
	g.hist[g.histN%dupHistorySize] = fp
	g.histN++
	if dup {
		g.streak++
	} else {
		g.streak = 0
	}
	buf := g.buf
	g.resetBlock()
	if dup {
		return nil, g.streak >= dupDisconnectStreak
	}
	return buf, false
}

// takeAll 取走当前块的全部标签，不做重复裁决（流结束/中止时尽量保留
// 已收到的数据）。
func (g *dupGuard) takeAll() []*flv.Tag {
	buf := g.buf
	g.resetBlock()
	return buf
}

// fingerprint 折叠块内全部内容指纹，再混入标签数以区分不同长度的块。
func (g *dupGuard) fingerprint() uint64 {
	return g.sum ^ uint64(g.count)*0x9E3779B97F4A7C15
}

// seen 判断指纹是否命中滑动窗口。
func (g *dupGuard) seen(fp uint64) bool {
	for i := range min(g.histN, dupHistorySize) {
		if g.hist[i] == fp {
			return true
		}
	}
	return false
}

func (g *dupGuard) resetBlock() {
	g.buf = nil
	g.bufBytes = 0
	g.sum = 0
	g.count = 0
	g.firstTs = 0
	g.lastTs = 0
}

// digestTag 计算单个 tag 的内容指纹（FNV-1a 64-bit）。
//
// 指纹输入：len(data) + tag.Type + data 前缀采样。
// 不混入 timestamp，避免 CDN 循环重放时“内容相同但时间戳被重写”导致
// 判重失效。
func digestTag(tag *flv.Tag) uint64 {
	const (
		fnvOffset = uint64(14695981039346656037)
		fnvPrime  = uint64(1099511628211)
	)

	h := fnvOffset
	h = (h ^ uint64(len(tag.Data))) * fnvPrime
	h = (h ^ uint64(tag.Type)) * fnvPrime

	// 截取前缀采样字节；小于阈值时按实际长度参与。
	data := tag.Data
	if len(data) > tagDigestSampleBytes {
		data = data[:tagDigestSampleBytes]
	}

	for _, b := range data {
		h = (h ^ uint64(b)) * fnvPrime
	}
	return h
}
