package data

import (
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/google/wire"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient)

// Data holds the long-lived storage/platform clients (sample habit). For
// the recorder these are the cookie-aware HTTP clients plus the shared
// risk-control helpers (WBI signer, buvid store).
type Data struct {
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
	apiClient := &http.Client{Timeout: 15 * time.Second}
	d := &Data{
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
	}
	return d, cleanup, nil
}
