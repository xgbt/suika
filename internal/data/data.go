package data

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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

// openDatabase opens the embedded database. Only the sqlite driver is
// supported; the source is a filesystem path (parent directories are
// created on demand).
func openDatabase(c *conf.Data_Database) (*gorm.DB, error) {
	driver := c.GetDriver()
	if driver != "sqlite" {
		return nil, fmt.Errorf("data: unsupported database driver %q, only \"sqlite\" is supported", driver)
	}
	source := c.GetSource()
	if source == "" {
		return nil, fmt.Errorf("data: database source is empty")
	}
	if dir := filepath.Dir(source); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("data: create database directory %q: %w", dir, err)
		}
	}
	db, err := gorm.Open(sqlite.Open(source), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("data: open sqlite database %q: %w", source, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("data: open sqlite connection pool: %w", err)
	}
	// A single connection avoids SQLITE_BUSY on the embedded database.
	sqlDB.SetMaxOpenConns(1)
	return db, nil
}
