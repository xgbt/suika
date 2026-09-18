package utils

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFn 返回一个写入 content 的写入函数，report 是上报给 WriteFileAtomic
// 的字节数（与 content 长度不一致时用于验证大小校验）。
func writeFn(content string, report int64) func(*bufio.Writer) (int64, error) {
	return func(bw *bufio.Writer) (int64, error) {
		if _, err := bw.Write([]byte(content)); err != nil {
			return 0, err
		}
		return report, nil
	}
}

// assertNoTmp 断言临时文件已被清理。
func assertNoTmp(t *testing.T, dst string) {
	t.Helper()
	if _, err := os.Stat(dst + ".tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp file must be removed (stat err = %v)", err)
	}
}

// assertContent 断言 dst 的内容与 want 一致。
func assertContent(t *testing.T, dst, want string) {
	t.Helper()
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read %s: %v", dst, err)
	}
	if string(got) != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
}

func TestWriteFileAtomicReplacesExistingFile(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "meta.json")
	if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := WriteFileAtomic(dst, writeFn("new", 3)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	assertContent(t, dst, "new")
	assertNoTmp(t, dst)
}

func TestWriteFileAtomicCreatesMissingFile(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "meta.json")

	if err := WriteFileAtomic(dst, writeFn("created", 7)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	assertContent(t, dst, "created")
	assertNoTmp(t, dst)
}

// 写入内容超过 1MB 缓冲：覆盖 bufio 满载刷盘的路径，并确认大小校验仍然成立。
func TestWriteFileAtomicWritesBeyondBufferSize(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "merged.flv")
	content := strings.Repeat("x", 3<<20)

	if err := WriteFileAtomic(dst, writeFn(content, int64(len(content)))); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	assertContent(t, dst, content)
	assertNoTmp(t, dst)
}

func TestWriteFileAtomicKeepsDestinationOnWriteError(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "meta.json")
	if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("disk on fire")

	err := WriteFileAtomic(dst, func(bw *bufio.Writer) (int64, error) {
		if _, werr := bw.Write([]byte("partial")); werr != nil {
			return 0, werr
		}
		return 0, writeErr
	})

	if !errors.Is(err, writeErr) {
		t.Fatalf("err = %v, want %v", err, writeErr)
	}
	assertContent(t, dst, "old")
	assertNoTmp(t, dst)
}

// 上报字节数与实际写入不一致时禁止改名：宁可不更新，也不能用不完整的文件
// 覆盖 dst。
func TestWriteFileAtomicRejectsSizeMismatch(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "meta.json")
	if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := WriteFileAtomic(dst, writeFn("new", 2))
	if err == nil {
		t.Fatal("WriteFileAtomic accepted a size mismatch")
	}

	assertContent(t, dst, "old")
	assertNoTmp(t, dst)
}
