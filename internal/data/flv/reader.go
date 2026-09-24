package flv

import (
	"context"
	"io"
)

// readBuffer 吸收 CDN 突发流量与消费方的短暂停顿。
const readBuffer = 64

// TagResult 是 ReadTags 投递的一次读取结果。
type TagResult struct {
	Tag *Tag  // 成功时非空
	Err error // 读取失败或流结束（io.EOF）时非空
}

// ReadTags 在后台协程中持续读取 FLV 标签并投递到返回的 channel
// 投递的 channel 有缓冲，避免短时间内的阻塞。出错（含 io.EOF）或 ctx 取消后协程退出并关闭 channel。
func ReadTags(ctx context.Context, r io.Reader) <-chan TagResult {
	ch := make(chan TagResult, readBuffer)
	go func() {
		defer close(ch)
		for {
			tag, err := ReadTag(r)
			select {
			case ch <- TagResult{Tag: tag, Err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	return ch
}
