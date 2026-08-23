package bili

import "testing"

func TestGetMixinKeyKnownVector(t *testing.T) {
	// 32 位 imgKey + 32 位 subKey，按置换表乱序后取前 32 字符。
	// 期望值按置换表手工推导，独立于实现路径。
	const imgKey = "7cd084941338484aae1ad9425b84077c"
	const subKey = "4932caff0ff746eab6f01bf08b70ac45"

	got := getMixinKey(imgKey, subKey)
	if got != "ea1db124af3c7062474693fa704f4ff8" {
		t.Fatalf("getMixinKey = %q", got)
	}
	if len(got) != 32 {
		t.Fatalf("len = %d, want 32", len(got))
	}
}

func TestGetMixinKeyShortInput(t *testing.T) {
	// combined 长度 4：置换表中只有下标 2、3、0、1 落在范围内，
	// 按表序输出。
	if got := getMixinKey("ab", "cd"); got != "cdab" {
		t.Fatalf("getMixinKey = %q, want %q", got, "cdab")
	}
}

func TestGetMixinKeyTruncatesTo32(t *testing.T) {
	got := getMixinKey("7cd084941338484aae1ad9425b84077c", "4932caff0ff746eab6f01bf08b70ac45")
	if len(got) > 32 {
		t.Fatalf("len = %d, want <= 32", len(got))
	}
}

func TestSanitizeWBIValue(t *testing.T) {
	cases := map[string]string{
		"a!b'c(d)e*f": "abcdef",
		"plain":       "plain",
		"中文值":         "中文值",
		"":            "",
		"!'()*":       "",
	}
	for in, want := range cases {
		if got := sanitizeWBIValue(in); got != want {
			t.Fatalf("sanitizeWBIValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestExtractKeyFromURL(t *testing.T) {
	got := extractKeyFromURL("https://i0.hdslb.com/bfs/wbi/7cd084941338484aae1ad9425b84077c.png")
	if got != "7cd084941338484aae1ad9425b84077c" {
		t.Fatalf("extractKeyFromURL = %q", got)
	}

	// 查询串不影响路径解析。
	got = extractKeyFromURL("https://i0.hdslb.com/bfs/wbi/abcdef.png?v=1")
	if got != "abcdef" {
		t.Fatalf("extractKeyFromURL with query = %q", got)
	}

	if got := extractKeyFromURL("://"); got != "" {
		t.Fatalf("extractKeyFromURL(malformed) = %q, want empty", got)
	}
}
