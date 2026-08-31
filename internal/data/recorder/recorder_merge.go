package recorder

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"suika/internal/data/flv"
)

// mergeSessionFiles 把一个会话的所有分段合并为单个文件：分段 FLV 合并为
// {base}.flv，分段弹幕 JSONL 按 part 顺序拼接为 {base}.danmu.jsonl。
//
// FLV 合并规则：
//   - 第 2 段起跳过 FLV 文件头；所有分段的 onMetaData 脚本标签一律跳过
//     （第一段的 duration 对合并后的文件是错的，播放器不依赖它）。
//   - 第 2 段起，段首由录制重新注入的序列头（AVC/AAC sequence header）
//     时间戳平移到合并边界，避免时间戳回跳；其余标签按原时间戳透传
//     （分段不重置时间戳，跨段本就是连续的绝对毫秒）。
//
// 铁律：输出先写临时文件，校验字节数无误后原子改名；只有改名成功后，
// 调用方才允许删除源分段。任何失败都会清理临时文件并原样保留源文件。
// 返回合并产物的文件名（相对 dir）；没有任何弹幕源时 danmakuName 为 ""。
func mergeSessionFiles(ctx context.Context, dir, base string, segs []segmentMeta) (videoName, danmakuName string, err error) {
	videoName = base + ".flv"
	if err := mergeFLV(ctx, dir, filepath.Join(dir, videoName), segs); err != nil {
		return "", "", err
	}
	danmakuName = base + ".danmu.jsonl"
	hasDanmu, err := mergeDanmaku(ctx, dir, filepath.Join(dir, danmakuName), segs)
	if err != nil {
		_ = os.Remove(filepath.Join(dir, videoName))
		return "", "", err
	}
	if !hasDanmu {
		danmakuName = ""
	}
	return videoName, danmakuName, nil
}

// mergeFLV 将各分段 FLV 合并写入 dst（临时文件 + 校验 + 原子改名）。
func mergeFLV(ctx context.Context, dir, dst string, segs []segmentMeta) error {
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("merge: create output: %w", err)
	}
	bw := bufio.NewWriterSize(out, 1<<20)
	var written int64
	// write 写入 b 并累计 written，供收尾时校验落盘字节数与目标文件大小一致。
	write := func(b []byte) error {
		n, werr := bw.Write(b)
		written += int64(n)
		return werr
	}

	// lastTs 是已写入的最后一个标签的时间戳，即下一段的合并边界。
	var lastTs int64
	mergeErr := func() error {
		for i, seg := range segs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			src := filepath.Join(dir, seg.Video)
			f, oerr := os.Open(src)
			if oerr != nil {
				return fmt.Errorf("merge: open segment %d (%s): %w", seg.Part, seg.Video, oerr)
			}
			if i == 0 {
				hdr, herr := flv.ParseHeader(f)
				if herr != nil {
					f.Close()
					return fmt.Errorf("merge: segment %d (%s): %w", seg.Part, seg.Video, herr)
				}
				if werr := write(hdr.Bytes()); werr != nil {
					f.Close()
					return fmt.Errorf("merge: write header: %w", werr)
				}
			} else if _, herr := flv.ParseHeader(f); herr != nil {
				// 第 2 段起跳过文件头，但仍要解析校验它。
				f.Close()
				return fmt.Errorf("merge: segment %d (%s): %w", seg.Part, seg.Video, herr)
			}
			// leading 标记段首的注入头区域：第 2 段起，重新注入的序列头
			// 时间戳要平移到边界；遇到第一个普通标签后该区域结束。
			leading := i > 0
			var timestampOffset int64
			offsetKnown := false
			rerr := func() error {
				for {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					tag, terr := flv.ReadTag(f)
					if terr == io.EOF {
						return nil
					}
					if terr != nil {
						return terr
					}
					switch {
					case tag.IsMetadata():
						// 所有分段的 onMetaData 一律跳过。
						continue
					case leading && (tag.IsAVCSequenceHeader() || tag.IsAACSequenceHeader()):
						tag.Timestamp = lastTs
					default:
						leading = false
						if i > 0 && !offsetKnown {
							offsetKnown = true
							if tag.Timestamp < lastTs {
								timestampOffset = lastTs - tag.Timestamp
							}
						}
						tag.Timestamp += timestampOffset
						lastTs = tag.Timestamp
					}
					if werr := write(tag.AppendTo(nil)); werr != nil {
						return werr
					}
				}
			}()
			f.Close()
			if rerr != nil {
				return fmt.Errorf("merge: segment %d (%s): %w", seg.Part, seg.Video, rerr)
			}
		}
		return nil
	}()
	if mergeErr == nil {
		mergeErr = bw.Flush()
	}
	if cerr := out.Close(); mergeErr == nil {
		mergeErr = cerr
	}
	if mergeErr == nil {
		// 校验：重算临时文件大小必须与逐标签累加的字节数一致。
		fi, serr := os.Stat(tmp)
		if serr != nil {
			mergeErr = serr
		} else if fi.Size() != written {
			mergeErr = fmt.Errorf("merge: output size %d does not match expected %d", fi.Size(), written)
		}
	}
	if mergeErr == nil {
		mergeErr = os.Rename(tmp, dst)
	}
	if mergeErr != nil {
		_ = os.Remove(tmp)
	}
	return mergeErr
}

// mergeDanmaku 将各分段弹幕 JSONL 按顺序拼接写入 dst。没有任何弹幕源
// 文件时不产出文件并返回 false。字节数校验同 mergeFLV。
func mergeDanmaku(ctx context.Context, dir, dst string, segs []segmentMeta) (bool, error) {
	var sources []string
	for _, seg := range segs {
		if seg.Danmaku == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, seg.Danmaku)); err == nil {
			sources = append(sources, filepath.Join(dir, seg.Danmaku))
		}
	}
	if len(sources) == 0 {
		return false, nil
	}
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return false, fmt.Errorf("merge: create danmaku output: %w", err)
	}
	var written int64
	for _, src := range sources {
		if cerr := ctx.Err(); cerr != nil {
			out.Close()
			_ = os.Remove(tmp)
			return false, cerr
		}
		in, oerr := os.Open(src)
		if oerr != nil {
			out.Close()
			_ = os.Remove(tmp)
			return false, fmt.Errorf("merge: open danmaku %s: %w", src, oerr)
		}
		n, cerr := io.Copy(out, in)
		in.Close()
		if cerr != nil {
			out.Close()
			_ = os.Remove(tmp)
			return false, fmt.Errorf("merge: copy danmaku %s: %w", src, cerr)
		}
		written += n
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	fi, err := os.Stat(tmp)
	if err != nil || fi.Size() != written {
		_ = os.Remove(tmp)
		if err == nil {
			err = fmt.Errorf("merge: danmaku output size %d does not match expected %d", fi.Size(), written)
		}
		return false, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	return true, nil
}

// allSegmentSourcesExist 判断所有分段的 FLV 源文件是否都在磁盘上，
// 用于决定一个 partial 会话是否值得重试合并。
func allSegmentSourcesExist(dir string, segs []segmentMeta) bool {
	for _, seg := range segs {
		if _, err := os.Stat(filepath.Join(dir, seg.Video)); err != nil {
			return false
		}
	}
	return len(segs) > 0
}

// sessionBaseFromMetaPath 从 meta.json 路径反推会话文件名前缀。
func sessionBaseFromMetaPath(metaPath string) string {
	return strings.TrimSuffix(filepath.Base(metaPath), ".meta.json")
}
