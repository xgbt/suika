package recorder

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"suika/internal/biz"
)

func TestSanitizeSegment(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"slashes", `a/b\\c`, "a_b_c"},
		{"all unsafe chars collapse", `\\/:*?"<>|`, "untitled"},
		{"whitespace runs collapse", "a  b\tc", "a_b_c"},
		{"chinese survives", "主播的 直播间", "主播的_直播间"},
		{"control chars", "a\x01b\x7fc", "a_b_c"},
		{"pure dots survive", "...", "..."},
		{"empty falls back", "", "untitled"},
		{"only whitespace falls back", "   ", "untitled"},
		{"edge underscores trimmed", "_a b_", "a_b"},
		{"long title no truncate", strings.Repeat("a", 70), strings.Repeat("a", 70)},
		{"long chinese no truncate", strings.Repeat("字", 40), strings.Repeat("字", 40)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeSegment(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeSegment(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.ContainsAny(got, `/\`) {
				t.Errorf("result %q contains a path separator", got)
			}
			if again := sanitizeSegment(got); again != got {
				t.Errorf("result not stable: second pass = %q", again)
			}
		})
	}
}

func TestNextPartNumber(t *testing.T) {
	const base = "20260811_2000_title"
	cases := []struct {
		name  string
		files []string
		dirOK bool // false => 指向不存在的目录
		want  int
	}{
		{"empty directory", nil, true, 1},
		{"missing directory", nil, false, 1},
		{
			name:  "two existing parts",
			files: []string{base + "_part1.flv", base + "_part2.flv"},
			dirOK: true,
			want:  3,
		},
		{
			name: "unrelated files ignored, padded, non-flv not counted",
			files: []string{
				base + "_part001.flv",       // 前导零编号：Atoi 能处理
				base + "_part002.mp4",       // 分段只认 .flv：mp4 不计入编号
				"other_part99.flv",          // 基座前缀不同
				base + "_partX.flv",         // 编号非数字
				base + "_part3.danmu.jsonl", // 扩展符不符
				"random.txt",
			},
			dirOK: true,
			want:  2, // max(1) + 1
		},
		{
			name:  "high part number",
			files: []string{base + "_part999.flv"},
			dirOK: true,
			want:  1000,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.dirOK {
				touchFiles(t, dir, tc.files...)
			} else {
				dir = filepath.Join(dir, "does-not-exist")
			}
			if got := nextPartNumber(sessionLayout{dir: dir, base: base}); got != tc.want {
				t.Fatalf("nextPartNumber = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSessionPaths(t *testing.T) {
	repo := newTestRepo(t, nil)
	lay, err := sessionPaths(repo.recordRoot, testSession())
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(repo.recordRoot, "42_tester", "2026-08-11")
	if lay.dir != wantDir {
		t.Fatalf("dir = %q, want %q", lay.dir, wantDir)
	}
	if want := "20260811_2000_stream_title"; lay.base != want {
		t.Fatalf("base = %q, want %q", lay.base, want)
	}
	// 会话身份只有一个表达：从 meta.json 路径还原出的布局必须与原布局一致，
	// 否则 RecoverPending 与录制路径会指向不同场次。
	if got := sessionLayoutFromMetaPath(lay.metaPath()); got != lay {
		t.Fatalf("layout round-trip = %+v, want %+v", got, lay)
	}

	if _, err := sessionPaths(repo.recordRoot, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("nil session err = %v, want ErrRoomInternal", err)
	}
	if _, err := sessionPaths(repo.recordRoot, &biz.RecordingSession{RoomID: 0}); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("zero room err = %v, want ErrRoomInternal", err)
	}
}

func TestSessionLayoutFileNames(t *testing.T) {
	lay := sessionLayout{dir: "/rec/42_x/2026-08-11", base: "20260811_2000_t"}

	if got, want := lay.segmentVideoName(2), "20260811_2000_t_part2.flv"; got != want {
		t.Errorf("segmentVideoName = %q, want %q", got, want)
	}
	if got, want := lay.segmentDanmakuName(2), "20260811_2000_t_part2.danmu.jsonl"; got != want {
		t.Errorf("segmentDanmakuName = %q, want %q", got, want)
	}
	if got, want := lay.mergedVideoName(), "20260811_2000_t.flv"; got != want {
		t.Errorf("mergedVideoName = %q, want %q", got, want)
	}
	if got, want := lay.mergedDanmakuName(), "20260811_2000_t.danmu.jsonl"; got != want {
		t.Errorf("mergedDanmakuName = %q, want %q", got, want)
	}
	if got, want := lay.metaPath(), filepath.Join(lay.dir, "20260811_2000_t.meta.json"); got != want {
		t.Errorf("metaPath = %q, want %q", got, want)
	}
	if got, want := lay.segmentVideoPath(2), filepath.Join(lay.dir, lay.segmentVideoName(2)); got != want {
		t.Errorf("segmentVideoPath = %q, want %q", got, want)
	}
}

// 文件名的生成（segmentVideoName）与编号探测（nextPartNumber）必须互为
// 逆运算，否则下一个分段会覆盖已有分段。合并产物 {base}.flv 不带 _part，
// 不得被计入编号。
func TestNextPartNumberMatchesGeneratedNames(t *testing.T) {
	lay := sessionLayout{dir: t.TempDir(), base: "20260811_2000_t"}
	touchFiles(
		t, lay.dir,
		lay.segmentVideoName(7),
		lay.segmentDanmakuName(9), // 弹幕文件不占用编号
		lay.mergedVideoName(),
		lay.mergedDanmakuName(),
		lay.base+".meta.json",
	)
	if got := nextPartNumber(lay); got != 8 {
		t.Fatalf("nextPartNumber = %d, want 8", got)
	}
}
