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
	unsafeChars       = regexp.MustCompile(`[\x00-\x1f\x7f\\/:*?"<>|\s\p{Z}]+`)
	partSuffixPattern = regexp.MustCompile(`_part(\d+)\.(flv|mp4)$`)
)

// sessionLayout 是一个录制会话在磁盘上的位置：会话目录 + 文件名前缀。
// 会话内所有文件名都由这两者派生，dir 与 base 因此不可能各说各话；会话
// 身份只有这一个表达，不需要再从 meta.json 路径反推回来。
type sessionLayout struct {
	dir  string // 会话目录
	base string // 文件名前缀，分段 / meta / 合并产物共享
}

// metaPath 返回会话 meta.json 的路径。
func (l sessionLayout) metaPath() string {
	return filepath.Join(l.dir, l.base+metaExt)
}

// filePath 返回会话目录下 name 的完整路径。
func (l sessionLayout) filePath(name string) string {
	return filepath.Join(l.dir, name)
}

// segmentVideoName / segmentDanmakuName 返回分段文件名（不含目录）。
func (l sessionLayout) segmentVideoName(part int) string {
	return fmt.Sprintf("%s_part%d%s", l.base, part, videoExt)
}

func (l sessionLayout) segmentDanmakuName(part int) string {
	return fmt.Sprintf("%s_part%d%s", l.base, part, danmakuExt)
}

// segmentVideoPath / segmentDanmakuPath 返回分段文件的完整路径。
func (l sessionLayout) segmentVideoPath(part int) string {
	return l.filePath(l.segmentVideoName(part))
}

func (l sessionLayout) segmentDanmakuPath(part int) string {
	return l.filePath(l.segmentDanmakuName(part))
}

// mergedVideoName / mergedDanmakuName 返回合并产物文件名（不含目录）。
func (l sessionLayout) mergedVideoName() string {
	return l.base + videoExt
}

func (l sessionLayout) mergedDanmakuName() string {
	return l.base + danmakuExt
}

// mergedVideoPath / mergedDanmakuPath 返回合并产物的完整路径。
func (l sessionLayout) mergedVideoPath() string {
	return l.filePath(l.mergedVideoName())
}

func (l sessionLayout) mergedDanmakuPath() string {
	return l.filePath(l.mergedDanmakuName())
}

// sessionPaths 计算会话的磁盘布局：所有分段、meta 与合并产物共享的目录和
// 文件名前缀（日期/时间/标题）。
//
//	recordings/
//	└── 12345_主播名/
//	    └── 2024-06-01/
//	        ├── 20240601_1504_直播标题.meta.json
//	        ├── 20240601_1504_直播标题_part1.flv
//	        └── 20240601_1504_直播标题_part1.danmu.jsonl
func sessionPaths(recordRoot string, session *biz.RecordingSession) (sessionLayout, error) {
	if session == nil || session.RoomID <= 0 {
		return sessionLayout{}, biz.ErrRoomInternal
	}

	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}

	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.StreamerName))
	return sessionLayout{
		dir:  filepath.Join(recordRoot, roomDir, start.Format("2006-01-02")),
		base: start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title),
	}, nil
}

// sessionLayoutFromMetaPath 从 meta.json 路径还原会话布局：RecoverPending
// 手上只有扫描到的 meta.json 路径，这是进入会话的唯一入口。
func sessionLayoutFromMetaPath(metaPath string) sessionLayout {
	return sessionLayout{
		dir:  filepath.Dir(metaPath),
		base: strings.TrimSuffix(filepath.Base(metaPath), metaExt),
	}
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
func nextPartNumber(lay sessionLayout) int {
	entries, err := os.ReadDir(lay.dir)
	if err != nil {
		return 1 // 目录尚未创建，本次就是 part1
	}
	maxPart := 0
	prefix := lay.base + "_part"
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
