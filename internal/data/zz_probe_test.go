package data

// 诊断用探针（临时文件，诊断完成后删除）。
// 运行方式：
//   SUIKA_PROBE=1 go test -mod=mod ./internal/data/ -run TestZZProbeDanmakuAuth -v -count=1
//
// 它以只读方式打开真实数据库取凭据（不打印任何凭据值），
// 复现生产弹幕 WS 认证路径，并对比多个变体，定位登录态下
// 弹幕服务器拒绝认证的原因。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

const zzProbeRoomID = int64(22389319)

func TestZZProbeDanmakuAuth(t *testing.T) {
	if os.Getenv("SUIKA_PROBE") == "" {
		t.Skip("set SUIKA_PROBE=1 to run the live probe")
	}

	// 1. 只读打开真实数据库取凭据。
	db, err := gorm.Open(sqlite.Open("../../data/suika.db?mode=ro"), &gorm.Config{Logger: glogger.Discard})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	var po credentialPO
	if err := db.First(&po, credentialSingletonID).Error; err != nil {
		t.Fatalf("read credential: %v", err)
	}
	cookie := po.Cookie
	t.Logf("stored cookie names: %s (len=%d)", zzCookieNames(cookie), len(cookie))

	// 2. 按生产方式组装 Data / liveClient。
	d := &Data{
		apiClient:    resty.New().SetTimeout(15 * time.Second),
		streamClient: resty.New(),
		cookie:       cookie,
	}
	d.signer = newWBISigner(d.apiClient, d.Cookie)
	d.buvids = newBuvidStore(d.apiClient)
	lc := &liveClient{data: d, risk: newRiskGuard(d.refreshRisk)}
	ctx := context.Background()

	// 3. 回退轮询路径（生产 GetRoomInfo）。
	roomInfo, err := lc.GetRoomInfo(ctx, zzProbeRoomID)
	if err != nil {
		t.Logf("GetRoomInfo (fallback poll path): ERR %v", err)
	} else {
		t.Logf("GetRoomInfo (fallback poll path): live=%v title-len=%d", roomInfo.Live, len(roomInfo.Title))
	}

	// 4. 生产 danmuInfo 路径。
	info, err := lc.danmuInfo(ctx, zzProbeRoomID)
	if err != nil {
		t.Fatalf("danmuInfo: %v", err)
	}
	t.Logf("danmuInfo: token-len=%d buvid-len=%d addresses=%v", len(info.token), len(info.buvid), info.addresses)

	// 5. 变体对比：逐个尝试 dial + auth。
	dedeUID := cookieValue(cookie, "DedeUserID")
	uid, _ := strconv.ParseInt(dedeUID, 10, 64)
	t.Logf("DedeUserID present: %v", dedeUID != "")

	variants := []struct {
		name   string
		cookie string
		uid    int64
		buvid  string
	}{
		{"PROD cookie+uid0+buvid", cookie, 0, info.buvid},
		{"no-cookie uid0 buvid", "", 0, info.buvid},
		{"cookie+realUID+buvid", cookie, uid, info.buvid},
		{"cookie+uid0+no-buvid", cookie, 0, ""},
	}
	addr := info.addresses[0]
	for _, v := range variants {
		err := zzProbeDial(ctx, addr, info.token, v.cookie, v.uid, zzProbeRoomID, v.buvid)
		if err != nil {
			t.Logf("variant %-26s -> FAIL: %v", v.name, err)
		} else {
			t.Logf("variant %-26s -> OK (auth accepted)", v.name)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 6. 直接走修复后的生产路径（danmakuConn.dialAndAuth）。
	prod := &danmakuConn{lc: lc, roomID: zzProbeRoomID}
	wsc, err := prod.dialAndAuth(ctx, addr, info.token, 3, info.buvid)
	if err != nil {
		t.Logf("FIXED prod dialAndAuth -> FAIL: %v", err)
	} else {
		wsc.Close()
		t.Logf("FIXED prod dialAndAuth -> OK (auth accepted)")
	}
}

func zzProbeDial(ctx context.Context, address, token, cookie string, uid, roomID int64, buvid string) error {
	header := http.Header{
		"User-Agent": {biliUserAgent},
		"Origin":     {"https://live.bilibili.com"},
		"Referer":    {fmt.Sprintf("https://live.bilibili.com/%d", roomID)},
	}
	if cookie != "" {
		header.Set("Cookie", cookie)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, address, header)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	body := map[string]any{
		"uid":      uid,
		"roomid":   roomID,
		"protover": 3,
		"platform": "web",
		"type":     2,
		"key":      token,
		"buvid":    buvid,
	}
	auth, _ := json.Marshal(body)
	if err := conn.WriteMessage(websocket.BinaryMessage, packPacket(operationAuth, 1, auth)); err != nil {
		return fmt.Errorf("write auth: %w", err)
	}
	if err := waitAuthSuccess(conn); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	return nil
}

func zzCookieNames(cookieHeader string) string {
	var names []string
	for item := range strings.SplitSeq(cookieHeader, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if parts[0] != "" {
			names = append(names, parts[0])
		}
	}
	return strings.Join(names, ",")
}
