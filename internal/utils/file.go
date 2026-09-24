// file.go 提供原子写文件的原语：先写临时文件，落盘并校验字节数后改名替换。
package utils

import (
	"bufio"
	"fmt"
	"os"
)

// WriteFileAtomic 将 fn 的输出写入 dst 的临时文件，校验大小后原子替换 dst。
// 失败时关闭句柄、删除临时文件，并保留 dst 原状。
func WriteFileAtomic(dst string, fn func(w *bufio.Writer) (int64, error)) (err error) {
	tmp := dst + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 1<<20)
	written, err := fn(bw)
	if err != nil {
		return err
	}
	// 依次刷新用户态缓冲区和文件内容，确保替换后的文件完整可读。
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// 大小不一致通常表示写入计数错误，禁止用不完整的文件覆盖 dst。
	fi, err := os.Stat(tmp)
	if err != nil {
		return err
	}
	if fi.Size() != written {
		return fmt.Errorf("output size %d does not match expected %d", fi.Size(), written)
	}

	return os.Rename(tmp, dst)
}
