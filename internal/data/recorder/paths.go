package recorder

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"suika/internal/biz"
)

const (
	videoExt   = ".flv"         // 分段/合并视频文件扩展名
	danmakuExt = ".danmu.jsonl" // 分段/合并弹幕文件扩展名
	metaExt    = ".meta.json"   // 会话 meta 文件扩展名
)

var (
	// unsafeChars 匹配文件名不安全字符：控制字符、路径分隔符及 Unicode 空白，
	// + 折叠连续匹配为单个下划线。
	unsafeChars = regexp.MustCompile(`[\x00-\x1f\x7f\\/:*?"<>|\s\p{Z}]+`)
	partSuffixPattern = regexp.MustCompile(`_part(\d+)\.(flv|mp4)$`)
)

// metaFilePath 返回会话 meta 文件的完整路径。
func metaFilePath(dir, base string) string {
	return filepath.Join(dir, base+metaExt)
}

// segmentVideoName 返回分段视频文件名（不含目录）。
func segmentVideoName(base string, part int) string {
	return fmt.Sprintf("%s_part%d%s", base, part, videoExt)
}

// segmentDanmakuName 返回分段弹幕文件名（不含目录）。
func segmentDanmakuName(base string, part int) string {
	return fmt.Sprintf("%s_part%d%s", base, part, danmakuExt)
}

// mergedVideoName 返回合并后视频文件名（不含目录）。
func mergedVideoName(base string) string {
	return base + videoExt
}

// mergedDanmakuName 返回合并后弹幕文件名（不含目录）。
func mergedDanmakuName(base string) string {
	return base + danmakuExt
}

// sessionPaths 计算会话目录和文件名前缀（所有分段与 meta 文件共享的日期/时间/标题前缀）。
//
// 目录结构示例：
//
//	recordings/
//	└── 12345_主播名/
//	    └── 2024-06-01/
//	        ├── 20240601_1504_直播标题.meta.json
//	        ├── 20240601_1504_直播标题_part1.flv
//	        └── 20240601_1504_直播标题_part1.danmu.jsonl
//
// 返回值：
//   - dir  : recordings/12345_主播名/2024-06-01
//   - base : 20240601_1504_直播标题
func sessionPaths(recordRoot string, session *biz.RecordingSession) (dir string, base string, err error) {
	if session == nil || session.RoomID <= 0 {
		return "", "", biz.ErrRoomInternal
	}

	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}

	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.StreamerName))
	dir = filepath.Join(recordRoot, roomDir, start.Format("2006-01-02"))
	base = start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title)
	return dir, base, nil
}

// sanitizeSegment 清理文件名并替换不安全字符。
func sanitizeSegment(s string) string {
	s = strings.Trim(unsafeChars.ReplaceAllString(s, "_"), "_")
	if s == "" {
		return "untitled"
	}
	return s
}

// nextPartNumber 扫描会话目录推导下一个分段编号, 同时覆盖重连和崩溃重启两种情况
func nextPartNumber(dir, base string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1 // 目录尚未创建，本次就是 part1
	}
	maxPart := 0
	prefix := base + "_part"
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue // 其他场次（日期或标题不同）的文件
		}
		// 正则未锚定 base，前缀检查才是限定本场次的关键；
		// 此处同时滤掉 .danmu.jsonl 这类同前缀的非分段文件。
		m := partSuffixPattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && n > maxPart {
			maxPart = n
		}
	}
	// 取最大值 +1 而非填补空洞：已归档的合并产物同样占用分段编号，复用会覆盖旧分段
	return maxPart + 1
}
