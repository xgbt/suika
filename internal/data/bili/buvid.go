package bili

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
)

// buvidTTL 是 finger/spi 指纹的缓存有效期。
const buvidTTL = 24 * time.Hour

// defaultBuvidSpiURL 是 B 站设备指纹端点。
const defaultBuvidSpiURL = "https://api.bilibili.com/x/frontend/finger/spi"

type cachedBuvid struct {
	buvid3    string
	buvid4    string
	expiresAt time.Time
}

// buvidStore 获取并缓存 B 站设备指纹（buvid3/buvid4），注入 cookie
// 头以通过 -352 风控。移植自 hikami-go/internal/biliutil/buvid.go。
type buvidStore struct {
	httpClient *resty.Client
	spiURL     string
	cache      map[string]cachedBuvid
	mu         sync.Mutex
}

func newBuvidStore(httpc *resty.Client) *buvidStore {
	return &buvidStore{
		httpClient: httpc,
		spiURL:     defaultBuvidSpiURL,
		cache:      make(map[string]cachedBuvid),
	}
}

// getBuvids 返回 cookieHeader 对应的 buvid3/buvid4，缓存 24 小时。
func (s *buvidStore) getBuvids(ctx context.Context, cookieHeader string) (buvid3, buvid4 string, err error) {
	if s == nil {
		return "", "", nil
	}
	now := time.Now()
	s.mu.Lock()
	if cached, ok := s.cache[cookieHeader]; ok && now.Before(cached.expiresAt) {
		s.mu.Unlock()
		return cached.buvid3, cached.buvid4, nil
	}
	s.mu.Unlock()

	req := s.httpClient.R().
		SetContext(ctx).
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", "https://www.bilibili.com").
		SetHeader("Origin", "https://www.bilibili.com")
	if cookieHeader != "" {
		req.SetHeader("Cookie", cookieHeader)
	}

	resp, err := req.Get(s.spiURL)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return "", "", fmt.Errorf("get buvids http status %d", resp.StatusCode())
	}

	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			B3 string `json:"b_3"`
			B4 string `json:"b_4"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return "", "", err
	}
	if result.Code != 0 {
		return "", "", fmt.Errorf("get buvids code=%d message=%s", result.Code, result.Message)
	}
	if result.Data.B3 == "" {
		return "", "", fmt.Errorf("get buvids returned empty b_3")
	}

	s.mu.Lock()
	s.cache[cookieHeader] = cachedBuvid{
		buvid3:    result.Data.B3,
		buvid4:    result.Data.B4,
		expiresAt: now.Add(buvidTTL),
	}
	s.mu.Unlock()
	return result.Data.B3, result.Data.B4, nil
}

// invalidate 丢弃 cookieHeader 的缓存指纹，使下次 getBuvids 重新获取
// （风控重试前使用）。
func (s *buvidStore) invalidate(cookieHeader string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.cache, cookieHeader)
	s.mu.Unlock()
}

// injectBuvids 以替换语义向 cookie 头追加 buvid3/buvid4：先剔除已有的
// buvid3=/buvid4= 段，让新指纹生效（B 站解析同名 cookie 的第一个）。
func injectBuvids(cookieHeader, buvid3, buvid4 string) string {
	var kept []string
	if cookieHeader != "" {
		for part := range strings.SplitSeq(cookieHeader, ";") {
			p := strings.TrimSpace(part)
			if p == "" || strings.HasPrefix(p, "buvid3=") || strings.HasPrefix(p, "buvid4=") {
				continue
			}
			kept = append(kept, p)
		}
	}
	if buvid3 != "" {
		kept = append(kept, "buvid3="+buvid3)
	}
	if buvid4 != "" {
		kept = append(kept, "buvid4="+buvid4)
	}
	return strings.Join(kept, "; ")
}
