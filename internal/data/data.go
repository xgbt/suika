package data

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-resty/resty/v2"
	"github.com/google/wire"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var ProviderSet = wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient, NewRoomRepo)

// Data 持有长生命周期的存储/平台客户端（模板惯例）。对录制器而言，
// 它们是携带 cookie 的 HTTP 客户端、共享的风控助手（WBI 签名器、
// buvid 存储），以及持久化 Room 列表的嵌入式数据库。
type Data struct {
	// db 是共享的 gorm 句柄（sqlite）。
	db *gorm.DB

	// apiClient 用于短小的 B 站 API 调用。
	apiClient *resty.Client
	// streamClient 拉取直播流；不设超时，因为读取是长生命周期的，
	// 取消经请求 context 传递。
	streamClient *resty.Client

	cookie string
	signer *wbiSigner
	buvids *buvidStore

	// 录制器配置（已应用默认值；proto 零值与未设置无法区分）。
	qualityQN          int
	recordInteractWord bool
	remuxEnabled       bool
	ffmpegPath         string
}

// NewData 构建共享客户端。开启转封装但找不到 ffmpeg 时快速失败
// （设计如此：启动期探测）。
func NewData(c *conf.Data, rc *conf.Recorder) (*Data, func(), error) {
	db, err := openDatabase(c.GetDatabase())
	if err != nil {
		return nil, nil, err
	}
	if err := db.AutoMigrate(&roomPO{}); err != nil {
		return nil, nil, fmt.Errorf("data: auto-migrate rooms table: %w", err)
	}

	apiClient := resty.New().SetTimeout(15 * time.Second)
	d := &Data{
		db:           db,
		apiClient:    apiClient,
		streamClient: resty.New(),
		cookie:       rc.GetCookie(),
		qualityQN:    10000,
		remuxEnabled: true,
	}
	d.signer = newWBISigner(apiClient, d.cookie)
	d.buvids = newBuvidStore(apiClient)

	if rc != nil {
		if rc.GetQualityQn() > 0 {
			d.qualityQN = int(rc.GetQualityQn())
		}
		if rc.GetDanmaku() != nil {
			d.recordInteractWord = rc.GetDanmaku().GetRecordInteractWord()
		}
		if rc.RemuxEnabled != nil {
			d.remuxEnabled = rc.GetRemuxEnabled()
		}
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
	if rc != nil && d.cookie == "" {
		log.Warn("recorder: no cookie configured; source quality may be unavailable")
	}

	cleanup := func() {
		log.Info("closing the data resources")
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return d, cleanup, nil
}

func openDatabase(c *conf.Data_Database) (*gorm.DB, error) {
	driver := c.GetDriver()
	if driver != "sqlite" {
		return nil, fmt.Errorf("data: unsupported database driver %q, only \"sqlite\" is supported", driver)
	}
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
