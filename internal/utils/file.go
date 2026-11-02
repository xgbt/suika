package utils

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic 将 fn 的输出原子地写入 dst：先写同目录临时文件，校验并落盘后 rename 替换。
// fn 须通过 w 写入全部内容，并返回写入的总字节数；失败时 dst 保持原状且不留临时文件。
func WriteFileAtomic(dst string, fn func(w *bufio.Writer) (int64, error)) (err error) {
	dir := filepath.Dir(dst)

	// 1. 同目录创建临时文件，保证 rename 不跨文件系统
	file, err := os.CreateTemp(dir, filepath.Base(dst)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", dst, err)
	}
	temp, committed := file.Name(), false

	// 2. 统一收尾：未提交（报错或 panic）则清理临时文件；报错时统一补充上下文
	defer func() {
		if committed {
			return
		}
		_ = file.Close()
		_ = os.Remove(temp)
		if err != nil {
			err = fmt.Errorf("atomic write %s: %w", dst, err)
		}
	}()

	// 3. 权限：覆盖已有文件时沿用其权限，否则用 0644（CreateTemp 默认是 0600）
	mode := os.FileMode(0o644)
	if fi, e := os.Stat(dst); e == nil {
		mode = fi.Mode().Perm()
	}
	if err = file.Chmod(mode); err != nil {
		return err
	}

	// 4. 写入并刷新缓冲
	bw := bufio.NewWriterSize(file, 1<<20)
	written, err := fn(bw)
	if err != nil {
		return err
	}
	if err = bw.Flush(); err != nil {
		return err
	}

	// 5. 校验大小，防止截断或计数错误
	fi, err := file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != written {
		return fmt.Errorf("size mismatch: got %d, want %d", fi.Size(), written)
	}

	// 6. 数据落盘后关闭（Close 的错误必须检查）
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}

	// 7. 原子替换，成功后不再清理
	if err = os.Rename(temp, dst); err != nil {
		return err
	}
	committed = true

	// 8. 持久化目录项，使 rename 掉电后仍有效（尽力而为，失败不影响结果）
	if d, e := os.Open(dir); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
