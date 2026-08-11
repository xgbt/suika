package data

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// errWBIKeyUnavailable means the WBI signing keys could not be fetched.
var errWBIKeyUnavailable = stderrors.New("wbi key unavailable")

// mixinKeyEncTab is the 64-element permutation table used by WBI signing.
// Ported from hikami-go/internal/biliutil/wbi.go.
var mixinKeyEncTab = [64]int{
	46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
	27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
	37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
	22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
}

// wbiSigner signs request URLs with w_rid/wts as required by Bilibili's
// -352 risk control. Keys are fetched from the nav API and cached 1 hour.
type wbiSigner struct {
	httpClient *http.Client
	cookie     string
	mu         sync.Mutex
	mixinKey   string
	updatedAt  time.Time
}

func newWBISigner(httpc *http.Client, cookie string) *wbiSigner {
	return &wbiSigner{httpClient: httpc, cookie: cookie}
}

// signURL appends wts and w_rid query parameters to rawURL.
func (s *wbiSigner) signURL(rawURL string) (string, error) {
	if err := s.ensureKeys(); err != nil {
		return "", err
	}

	s.mu.Lock()
	mixinKey := s.mixinKey
	s.mu.Unlock()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}

	query := parsed.Query()
	query.Set("wts", strconv.FormatInt(time.Now().Unix(), 10))

	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(sanitizeWBIValue(query.Get(k)))
	}

	hash := md5.Sum([]byte(sb.String() + mixinKey))
	query.Set("w_rid", hex.EncodeToString(hash[:]))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// refreshKeys forces a key refresh from the nav API.
func (s *wbiSigner) refreshKeys() error {
	return s.fetchKeys()
}

func (s *wbiSigner) ensureKeys() error {
	s.mu.Lock()
	fresh := s.mixinKey != "" && time.Since(s.updatedAt) < time.Hour
	s.mu.Unlock()
	if fresh {
		return nil
	}
	return s.fetchKeys()
}

func (s *wbiSigner) fetchKeys() error {
	const navURL = "https://api.bilibili.com/x/web-interface/nav"
	req, err := http.NewRequest(http.MethodGet, navURL, nil)
	if err != nil {
		return fmt.Errorf("create nav request: %w", err)
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", "https://www.bilibili.com")
	if s.cookie != "" {
		req.Header.Set("Cookie", s.cookie)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: nav request: %v", errWBIKeyUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%w: nav http status %d", errWBIKeyUnavailable, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%w: read nav response: %v", errWBIKeyUnavailable, err)
	}

	var navResp struct {
		Code int `json:"code"`
		Data struct {
			WbiImg struct {
				ImgURL string `json:"img_url"`
				SubURL string `json:"sub_url"`
			} `json:"wbi_img"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &navResp); err != nil {
		return fmt.Errorf("%w: parse nav response: %v", errWBIKeyUnavailable, err)
	}

	imgKey := extractKeyFromURL(navResp.Data.WbiImg.ImgURL)
	subKey := extractKeyFromURL(navResp.Data.WbiImg.SubURL)
	if imgKey == "" || subKey == "" {
		return errWBIKeyUnavailable
	}

	mixinKey := getMixinKey(imgKey, subKey)
	s.mu.Lock()
	s.mixinKey = mixinKey
	s.updatedAt = time.Now()
	s.mu.Unlock()
	return nil
}

// extractKeyFromURL extracts the file name without the .png suffix.
func extractKeyFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(path.Base(u.Path), ".png")
}

// getMixinKey derives the 32-char mixin key from imgKey+subKey.
func getMixinKey(imgKey, subKey string) string {
	combined := imgKey + subKey
	var result strings.Builder
	for _, idx := range mixinKeyEncTab {
		if idx < len(combined) {
			result.WriteByte(combined[idx])
		}
	}
	mixed := result.String()
	if len(mixed) > 32 {
		return mixed[:32]
	}
	return mixed
}

// sanitizeWBIValue strips the special characters !'()* from a value.
func sanitizeWBIValue(v string) string {
	var sb strings.Builder
	for _, ch := range v {
		if !strings.ContainsRune("!'()*", ch) {
			sb.WriteRune(ch)
		}
	}
	return sb.String()
}
