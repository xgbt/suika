package data

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
)

// errWBIKeyUnavailable 表示 WBI 签名密钥获取失败。
var errWBIKeyUnavailable = stderrors.New("wbi key unavailable")

// mixinKeyEncTab 是 WBI 签名使用的 64 元素置换表。
// 移植自 hikami-go/internal/biliutil/wbi.go。
var mixinKeyEncTab = [64]int{
	46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
	27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
	37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
	22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
}

// wbiSigner 按 B 站 -352 风控要求为请求 URL 附加 w_rid/wts 签名。
// 密钥从 nav API 获取并缓存 1 小时。
type wbiSigner struct {
	httpClient *resty.Client
	cookie     string
	mu         sync.Mutex
	mixinKey   string
	updatedAt  time.Time
}

func newWBISigner(httpc *resty.Client, cookie string) *wbiSigner {
	return &wbiSigner{httpClient: httpc, cookie: cookie}
}

// signURL 为 rawURL 追加 wts 和 w_rid 查询参数。
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

// refreshKeys 强制从 nav API 刷新密钥。
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
	req := s.httpClient.R().
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", "https://www.bilibili.com").
		SetDoNotParseResponse(true)
	if s.cookie != "" {
		req.SetHeader("Cookie", s.cookie)
	}

	resp, err := req.Get(navURL)
	if err != nil {
		return fmt.Errorf("%w: nav request: %v", errWBIKeyUnavailable, err)
	}
	bodyReader := resp.RawBody()
	if bodyReader == nil {
		return fmt.Errorf("%w: nav response body is empty", errWBIKeyUnavailable)
	}
	defer bodyReader.Close()

	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return fmt.Errorf("%w: nav http status %d", errWBIKeyUnavailable, resp.StatusCode())
	}

	body, err := io.ReadAll(bodyReader)
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

// extractKeyFromURL 提取去掉 .png 后缀的文件名。
func extractKeyFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(path.Base(u.Path), ".png")
}

// getMixinKey 由 imgKey+subKey 推导 32 字符的 mixin key。
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

// sanitizeWBIValue 剔除值中的特殊字符 !'()*。
func sanitizeWBIValue(v string) string {
	var sb strings.Builder
	for _, ch := range v {
		if !strings.ContainsRune("!'()*", ch) {
			sb.WriteRune(ch)
		}
	}
	return sb.String()
}
