package bili

import "testing"

func TestInjectBuvidsReplacesExisting(t *testing.T) {
	got := injectBuvids("a=1; buvid3=old3; b=2; buvid4=old4", "new3", "new4")
	want := "a=1; b=2; buvid3=new3; buvid4=new4"
	if got != want {
		t.Fatalf("injectBuvids = %q, want %q", got, want)
	}
}

func TestInjectBuvidsAppendsToEmpty(t *testing.T) {
	if got := injectBuvids("", "x3", "x4"); got != "buvid3=x3; buvid4=x4" {
		t.Fatalf("injectBuvids = %q", got)
	}
}

func TestInjectBuvidsSkipsEmptyValues(t *testing.T) {
	if got := injectBuvids("a=1", "x3", ""); got != "a=1; buvid3=x3" {
		t.Fatalf("injectBuvids = %q", got)
	}
	if got := injectBuvids("", "", ""); got != "" {
		t.Fatalf("injectBuvids = %q, want empty", got)
	}
}

func TestInjectBuvidsTrimsWhitespaceAndEmptyEntries(t *testing.T) {
	got := injectBuvids(" a=1 ;;  b=2 ", "x3", "x4")
	want := "a=1; b=2; buvid3=x3; buvid4=x4"
	if got != want {
		t.Fatalf("injectBuvids = %q, want %q", got, want)
	}
}

func TestCookieValue(t *testing.T) {
	header := "a=1; buvid3=abc; b=2"
	if got := cookieValue(header, "buvid3"); got != "abc" {
		t.Fatalf("cookieValue = %q, want abc", got)
	}
	if got := cookieValue(header, "missing"); got != "" {
		t.Fatalf("cookieValue = %q, want empty", got)
	}
	if got := cookieValue("", "buvid3"); got != "" {
		t.Fatalf("cookieValue(empty) = %q, want empty", got)
	}
	// 值本身含 '='：只按第一个 '=' 切分。
	if got := cookieValue("k=a=b", "k"); got != "a=b" {
		t.Fatalf("cookieValue = %q, want a=b", got)
	}
}
