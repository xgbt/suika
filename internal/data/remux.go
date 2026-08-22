package data

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

// remuxWithRetry 将 FLV 分段转封装为 MP4（流拷贝，不重编码），并注入
// title/artist/date 容器元数据。失败时附加 -fflags +discardcorrupt
// 重试一次。此处绝不改动源文件；只有输出验证通过后才会删除源文件。
func remuxWithRetry(ctx context.Context, ffmpegPath, src, dst, title, artist string, liveStart int64) error {
	date := time.Unix(liveStart, 0).Format(time.DateTime)
	if err := runFFmpeg(ctx, ffmpegPath, src, dst, title, artist, date, false); err == nil {
		return nil
	} else {
		log.Warn("remux failed, retrying with discardcorrupt", "file", src, "err", err)
	}
	return runFFmpeg(ctx, ffmpegPath, src, dst, title, artist, date, true)
}

func runFFmpeg(ctx context.Context, ffmpegPath, src, dst, title, artist, date string, discardCorrupt bool) error {
	args := []string{"-hide_banner", "-loglevel", "error", "-y"}
	if discardCorrupt {
		args = append(args, "-fflags", "+discardcorrupt")
	}
	args = append(args, "-i", src, "-c", "copy")
	if title != "" {
		args = append(args, "-metadata", "title="+title)
	}
	if artist != "" {
		args = append(args, "-metadata", "artist="+artist)
	}
	if date != "" {
		args = append(args, "-metadata", "date="+date)
	}
	args = append(args, dst)
	cmd := exec.CommandContext(ctx, ffmpegPath, args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %w: %s", err, string(output))
	}
	return nil
}
