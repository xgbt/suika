package data

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// writeFakeFFmpeg installs an executable shell script standing in for the
// ffmpeg binary so remux tests run without ffmpeg installed. The script
// appends each invocation's arguments to argsFile, counts invocations in
// countFile, exits 1 for the first failTimes calls, then writes a non-empty
// payload to its final argument (the remux output path).
func writeFakeFFmpeg(t *testing.T, dir string, failTimes int) (ffmpeg, argsFile, countFile string) {
	t.Helper()
	argsFile = filepath.Join(dir, "args.txt")
	countFile = filepath.Join(dir, "count.txt")
	ffmpeg = filepath.Join(dir, "fake-ffmpeg.sh")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" >> %q
n=$(cat %q 2>/dev/null || echo 0)
n=$((n+1))
echo $n > %q
if [ "$n" -le %d ]; then
	echo "fake ffmpeg failure" >&2
	exit 1
fi
for last in "$@"; do :; done
printf 'FAKE_MP4_DATA' > "$last"
`, argsFile, countFile, countFile, failTimes)
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return ffmpeg, argsFile, countFile
}

func readCount(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRemuxWithRetrySuccessFirstAttempt(t *testing.T) {
	dir := t.TempDir()
	ffmpeg, argsFile, countFile := writeFakeFFmpeg(t, dir, 0)
	src := filepath.Join(dir, "in.flv")
	dst := filepath.Join(dir, "out.mp4")
	if err := os.WriteFile(src, []byte("fake flv"), 0o644); err != nil {
		t.Fatal(err)
	}

	liveStart := time.Date(2026, 8, 11, 20, 0, 0, 0, time.UTC).Unix()
	if err := remuxWithRetry(context.Background(), ffmpeg, src, dst, "Live Title", "streamer", liveStart); err != nil {
		t.Fatalf("remuxWithRetry: %v", err)
	}
	if out, err := os.ReadFile(dst); err != nil || string(out) != "FAKE_MP4_DATA" {
		t.Fatalf("dst content = %q, %v", out, err)
	}
	if n := readCount(t, countFile); n != 1 {
		t.Fatalf("invocations = %d, want 1 (no retry on success)", n)
	}

	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	s := string(args)
	for _, want := range []string{"-hide_banner", "-loglevel", "error", "-y", "-i", src, "-c", "copy",
		"title=Live Title", "artist=streamer", "date=" + time.Unix(liveStart, 0).Format("2006-01-02 15:04:05")} {
		if !strings.Contains(s, want) {
			t.Errorf("args missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "discardcorrupt") {
		t.Errorf("first attempt must not pass discardcorrupt:\n%s", s)
	}
	if !strings.HasSuffix(strings.TrimRight(s, "\n"), dst) {
		t.Errorf("output path must be the final argument:\n%s", s)
	}
}

func TestRemuxWithRetryRetriesOnceWithDiscardCorrupt(t *testing.T) {
	dir := t.TempDir()
	ffmpeg, argsFile, countFile := writeFakeFFmpeg(t, dir, 1) // first call fails
	src := filepath.Join(dir, "in.flv")
	dst := filepath.Join(dir, "out.mp4")
	if err := os.WriteFile(src, []byte("fake flv"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := remuxWithRetry(context.Background(), ffmpeg, src, dst, "", "", 0); err != nil {
		t.Fatalf("remuxWithRetry: %v", err)
	}
	if n := readCount(t, countFile); n != 2 {
		t.Fatalf("invocations = %d, want 2 (one retry)", n)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "+discardcorrupt") {
		t.Errorf("retry must pass -fflags +discardcorrupt:\n%s", args)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Errorf("dst missing after successful retry: %v", err)
	}
}

func TestRemuxWithRetryGivesUpAfterOneRetry(t *testing.T) {
	dir := t.TempDir()
	ffmpeg, _, countFile := writeFakeFFmpeg(t, dir, 999) // always fails
	src := filepath.Join(dir, "in.flv")
	dst := filepath.Join(dir, "out.mp4")
	if err := os.WriteFile(src, []byte("fake flv"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := remuxWithRetry(context.Background(), ffmpeg, src, dst, "t", "a", 1)
	if err == nil {
		t.Fatal("want error when every attempt fails")
	}
	if !strings.Contains(err.Error(), "ffmpeg") || !strings.Contains(err.Error(), "fake ffmpeg failure") {
		t.Fatalf("err = %q, want wrapped ffmpeg output", err)
	}
	if n := readCount(t, countFile); n != 2 {
		t.Fatalf("invocations = %d, want 2 (initial + one retry)", n)
	}
	if _, serr := os.Stat(dst); !os.IsNotExist(serr) {
		t.Fatalf("dst must not exist after total failure (stat err = %v)", serr)
	}
	// the source is never touched by remux
	if _, serr := os.Stat(src); serr != nil {
		t.Fatalf("src must survive: %v", serr)
	}
}

func TestRemuxWithRetryMissingBinary(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.flv")
	if err := os.WriteFile(src, []byte("fake flv"), 0o644); err != nil {
		t.Fatal(err)
	}
	bogus := filepath.Join(dir, "no-such-ffmpeg")
	if err := remuxWithRetry(context.Background(), bogus, src, filepath.Join(dir, "out.mp4"), "", "", 0); err == nil {
		t.Fatal("want error for missing ffmpeg binary")
	}
}
