package data

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// buvidTTL is the cache lifetime for finger/spi fingerprints.
const buvidTTL = 24 * time.Hour

// defaultBuvidSpiURL is the Bilibili device-fingerprint endpoint.
const defaultBuvidSpiURL = "https://api.bilibili.com/x/frontend/finger/spi"

type cachedBuvid struct {
	buvid3    string
	buvid4    string
	expiresAt time.Time
}

// buvidStore fetches and caches Bilibili device fingerprints
// (buvid3/buvid4), injected into cookie headers to pass -352 risk
// control. Ported from hikami-go/internal/biliutil/buvid.go.
type buvidStore struct {
	httpClient *http.Client
	spiURL     string
	cache      map[string]cachedBuvid
	mu         sync.Mutex
}

func newBuvidStore(httpc *http.Client) *buvidStore {
	return &buvidStore{
		httpClient: httpc,
		spiURL:     defaultBuvidSpiURL,
		cache:      make(map[string]cachedBuvid),
	}
}

// getBuvids returns the buvid3/buvid4 pair for cookieHeader, cached 24h.
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.spiURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", "https://www.bilibili.com")
	req.Header.Set("Origin", "https://www.bilibili.com")
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("get buvids http status %d", resp.StatusCode)
	}

	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			B3 string `json:"b_3"`
			B4 string `json:"b_4"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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

// invalidate drops the cached fingerprints for cookieHeader so the next
// getBuvids refetches (used before risk-control retries).
func (s *buvidStore) invalidate(cookieHeader string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.cache, cookieHeader)
	s.mu.Unlock()
}

// injectBuvids appends buvid3/buvid4 to the cookie header with replace
// semantics: existing buvid3=/buvid4= segments are dropped first so the
// fresh fingerprint wins (Bilibili parses the first same-name cookie).
func injectBuvids(cookieHeader, buvid3, buvid4 string) string {
	var kept []string
	if cookieHeader != "" {
		for _, part := range strings.Split(cookieHeader, ";") {
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
