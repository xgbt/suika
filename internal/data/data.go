package data

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/google/wire"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient, NewRoomRepo)

// Data holds the long-lived storage/platform clients (sample habit). For
// the recorder these are the cookie-aware HTTP clients plus the shared
// risk-control helpers (WBI signer, buvid store), and the embedded
// database that persists the room list.
type Data struct {
	// db is the shared gorm handle (sqlite).
	db *gorm.DB

	// apiClient serves short Bilibili API calls.
	apiClient *http.Client
	// streamClient pulls live streams; no timeout because reads are
	// long-lived and cancellation flows through the request context.
	streamClient *http.Client

	cookie string
	signer *wbiSigner
	buvids *buvidStore

	// recorder settings resolved with defaults applied (proto zero
	// values are indistinguishable from unset).
	qualityQN          int
	recordInteractWord bool
	remuxEnabled       bool
	ffmpegPath         string
}

// NewData builds the shared clients. It fails fast when remux is enabled
// but ffmpeg is missing (design: startup probe).
func NewData(c *conf.Data, rc *conf.Recorder) (*Data, func(), error) {
	db, err := openDatabase(c.GetDatabase())
	if err != nil {
		return nil, nil, err
	}
	if err := db.AutoMigrate(&roomPO{}); err != nil {
		return nil, nil, fmt.Errorf("data: auto-migrate rooms table: %w", err)
	}

	apiClient := &http.Client{Timeout: 15 * time.Second}
	d := &Data{
		db:           db,
		apiClient:    apiClient,
		streamClient: &http.Client{},
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
	// A single connection avoids SQLITE_BUSY on the embedded database.
	sqlDB.SetMaxOpenConns(1)
	return db, nil
}

// ensureSQLiteDir validates the configured sqlite source and prepares its
// parent directory.
//
// Expected source format is a plain sqlite file path, for example:
//   - ./data/suika.db
//   - /var/lib/suika/suika.db
//
// A leading "file:" prefix is tolerated and stripped.
//
// It returns the normalized file path that should be passed to sqlite.Open.
// The database file itself is not created here; sqlite will create it on open
// when it does not exist.
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

// sqliteFilePath validates and normalizes the sqlite database source as a
// filesystem file path.
//
// Rules:
//   - source must be non-empty after trimming spaces.
//   - a leading "file:" prefix is tolerated and stripped for familiarity
//     with sqlite DSNs, but file URIs with an authority ("file://...")
//     are rejected — only plain file paths are supported.
//   - query parameters are rejected to keep config semantics simple and
//     deterministic (we only support plain file paths).
//   - directory-like inputs (for example "./data/" or "/") are rejected.
//
// The returned value is a cleaned path suitable for filepath operations and
// sqlite.Open.
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
