package data

import (
	stderrors "errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/bili"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/google/wire"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var ProviderSet = wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient, NewRoomRepo, NewCredentialRepo, NewPassportClient)

// Data 持有长生命周期的存储/平台客户端（模板惯例）：共享的 sqlite
// 句柄、与 B 站平台交互的共享客户端（bili 子包），以及录制器配置。
type Data struct {
	// db 是共享的 gorm 句柄（sqlite）。
	db *gorm.DB

	// bili 是与 B 站平台交互的共享客户端：携带 cookie 的 HTTP 客户端、
	// 当前生效的登录态与风控助手（WBI 签名器、buvid 存储）。
	bili *bili.Client

	// 录制器配置（已应用默认值；proto 零值与未设置无法区分）。
	remuxEnabled bool
	ffmpegPath   string
}

// NewLiveClient 提供 biz.LiveClient；B 站直播流量全部实现在 bili 子包。
func NewLiveClient(d *Data) biz.LiveClient { return bili.NewLiveClient(d.bili) }

// NewPassportClient 提供 biz.PassportClient；实现位于 bili 子包。
func NewPassportClient(d *Data) biz.PassportClient { return bili.NewPassportClient(d.bili) }

// Cookie 返回当前生效的 B 站 Cookie 头；未登录为 ""。
// 登录态由 bili 客户端持有，这里只是读快照的委托。
func (d *Data) Cookie() string { return d.bili.Cookie() }

// NewData 构建共享客户端。开启转封装但找不到 ffmpeg 时快速失败
// （设计如此：启动期探测）。
func NewData(c *conf.Data, rc *conf.Recorder) (*Data, func(), error) {
	db, err := openDatabase(c.GetDatabase())
	if err != nil {
		return nil, nil, err
	}
	if err := db.AutoMigrate(&roomPO{}, &credentialPO{}); err != nil {
		return nil, nil, fmt.Errorf("data: auto-migrate tables: %w", err)
	}

	// 登录凭据只来自数据库（recorder.cookie 已退役）。
	cookie, err := loadCredentialCookie(db)
	if err != nil {
		return nil, nil, err
	}

	d := &Data{
		db:           db,
		bili:         bili.NewClient(cookie),
		remuxEnabled: true,
	}

	if rc != nil && rc.RemuxEnabled != nil {
		d.remuxEnabled = rc.GetRemuxEnabled()
	}
	if rc != nil && d.remuxEnabled {
		ffmpegPath, err := exec.LookPath("ffmpeg")
		if err != nil {
			return nil, nil, fmt.Errorf("recorder: remux enabled but ffmpeg not found in PATH: %w", err)
		}
		d.ffmpegPath = ffmpegPath
		if _, err := exec.LookPath("ffprobe"); err != nil {
			log.Warn("ffprobe not found; remux verification limited to output existence")
		}
	}
	if rc != nil && rc.GetCookie() != "" {
		log.Warn("recorder: config field recorder.cookie is deprecated and ignored; the credential is managed in the database via web QR login")
	}
	if cookie == "" {
		log.Warn("data: no bilibili credential in the database; log in from the web UI to enable source quality")
	}

	cleanup := func() {
		log.Info("closing the data resources")
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return d, cleanup, nil
}

// loadCredentialCookie 启动时读取凭据单例行；无行返回空串。
func loadCredentialCookie(db *gorm.DB) (string, error) {
	var po credentialPO
	err := db.First(&po, credentialSingletonID).Error
	if stderrors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("data: load credential: %w", err)
	}
	return po.Cookie, nil
}

func openDatabase(c *conf.Data_Database) (*gorm.DB, error) {
	source := c.GetSource()
	if source == "" {
		return nil, fmt.Errorf("data: database source is empty")
	}

	filePath, err := ensureSQLiteDir(source)
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(sqlite.Open(filePath), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("data: open sqlite database %q: %w", filePath, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("data: open sqlite connection pool: %w", err)
	}
	// 单连接避免嵌入式数据库出现 SQLITE_BUSY。
	sqlDB.SetMaxOpenConns(1)
	return db, nil
}

// ensureSQLiteDir 校验配置的 sqlite 数据源并创建其父目录。
//
// 期望的数据源是纯 sqlite 文件路径，例如：
//   - ./data/suika.db
//   - /var/lib/suika/suika.db
//
// 允许并剥离开头的 "file:" 前缀。
//
// 返回规范化后的文件路径，应直接传给 sqlite.Open。这里不创建数据库
// 文件本身；文件不存在时 sqlite 会在打开时创建。
func ensureSQLiteDir(source string) (string, error) {
	filePath, err := sqliteFilePath(source)
	if err != nil {
		return "", err
	}
	if dir := filepath.Dir(filePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("data: create database directory %q: %w", dir, err)
		}
	}
	return filePath, nil
}

// sqliteFilePath 校验并把 sqlite 数据源规范化为文件系统路径。
//
// 规则：
//   - 数据源去除首尾空白后不得为空。
//   - 允许并剥离开头的 "file:" 前缀（照顾 sqlite DSN 的写法），但拒绝
//     带 authority 的 "file://..." URI —— 只支持纯文件路径。
//   - 拒绝查询参数，保持配置语义简单、确定（只支持纯文件路径）。
//   - 拒绝目录形式的输入（如 "./data/" 或 "/"）。
//
// 返回值是清理后的路径，可直接用于 filepath 操作和 sqlite.Open。
func sqliteFilePath(source string) (string, error) {
	pathPart := strings.TrimSpace(source)
	if pathPart == "" {
		return "", fmt.Errorf("data: database source is empty")
	}
	if rest, ok := strings.CutPrefix(pathPart, "file:"); ok {
		if strings.HasPrefix(rest, "//") {
			return "", fmt.Errorf("data: invalid database source %q: file URIs with an authority are not supported, use a plain file path like ./data/suika.db", source)
		}
		pathPart = rest
	}
	if _, _, ok := strings.Cut(pathPart, "?"); ok {
		return "", fmt.Errorf("data: invalid database source %q: query parameters are not supported, use a plain file path like ./data/suika.db", source)
	}
	if pathPart == "" {
		return "", fmt.Errorf("data: invalid database source %q", source)
	}
	if strings.HasSuffix(pathPart, "/") || strings.HasSuffix(pathPart, string(filepath.Separator)) {
		return "", fmt.Errorf("data: invalid database source %q: expected a file path, got a directory path", source)
	}
	pathPart = filepath.Clean(pathPart)
	if pathPart == "." || pathPart == string(filepath.Separator) {
		return "", fmt.Errorf("data: invalid database source %q", source)
	}
	if stat, err := os.Stat(pathPart); err == nil && stat.IsDir() {
		return "", fmt.Errorf("data: invalid database source %q: expected a file path, got a directory", source)
	}
	return pathPart, nil
}
