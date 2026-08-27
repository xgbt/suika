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

var (
	// unsafeChars 匹配文件名不安全字符：控制字符、路径分隔符及 Unicode 空白，
	// + 折叠连续匹配为单个下划线。
	unsafeChars       = regexp.MustCompile(`[\x00-\x1f\x7f\\/:*?"<>|\s\p{Z}]+`)
	partSuffixPattern = regexp.MustCompile(`_part(\d+)\.(flv|mp4)$`)
)

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
func sessionPaths(recordRoot string, session *biz.RecordingSession) (string, string, error) {
	if session == nil || session.RoomID <= 0 {
		return "", "", biz.ErrRoomInternal
	}

	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}

	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.StreamerName))
	dir := filepath.Join(recordRoot, roomDir, start.Format("2006-01-02"))
	base := start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title)
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

// nextPartNumber 扫描会话目录推导下一个分段编号
// 同时覆盖重连和崩溃重启两种情况
func nextPartNumber(dir, base string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	maxPart := 0
	prefix := base + "_part"
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		m := partSuffixPattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && n > maxPart {
			maxPart = n
		}
	}
	return maxPart + 1
}
