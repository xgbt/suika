// danmaku_event.go 把弹幕消息的 JSON 载荷解析为 biz.DanmakuEvent：弹幕、
// 礼物、醒目留言、上舰与进场特效。解析失败一律返回 nil（事件被丢弃）。
package bili

import (
	"encoding/json"
	"strconv"
	"time"

	"suika/internal/biz"
)

// parseDanmakuEvent 解析 DANMU_MSG：弹幕文本、发送者、模式与颜色。
// 载荷形状是数组（info[0]=弹幕元数据, info[1]=文本, info[2]=用户信息），
// 字段缺失或形状不符时返回 nil（该事件被丢弃）。
// info[0][4] 是平台侧的发送时刻（unix 毫秒），比接收时刻更贴近视频时间
// 轴（录制积压、网络抖动时差异明显），解析为 SendTs 供切片对齐；缺失或
// 非正数时保持 0（未知）。
func parseDanmakuEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Info []any `json:"info"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || len(m.Info) < 3 {
		return nil
	}
	text, _ := m.Info[1].(string)
	if text == "" {
		return nil
	}
	ev := &biz.DanmakuEvent{TS: receivedAt, Type: biz.EventDanmaku, Text: text, Raw: raw, Mode: 1}
	if user, ok := m.Info[2].([]any); ok && len(user) >= 2 {
		ev.UID = toInt64(user[0])
		ev.Uname, _ = user[1].(string)
	}
	if meta, ok := m.Info[0].([]any); ok {
		if len(meta) > 1 {
			if mode := int32(toInt64(meta[1])); mode > 0 {
				ev.Mode = mode
			}
		}
		if len(meta) > 3 {
			ev.Color = int32(toInt64(meta[3]))
		}
		if len(meta) > 4 {
			if sendTs := toInt64(meta[4]); sendTs > 0 {
				ev.SendTS = sendTs
			}
		}
	}
	return ev
}

// parseGiftEvent 解析 SEND_GIFT（礼物）事件。
func parseGiftEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID      int64  `json:"uid"`
			Uname    string `json:"uname"`
			GiftName string `json:"giftName"`
			Num      int32  `json:"num"`
			Price    int64  `json:"price"`
			CoinType string `json:"coin_type"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventGift, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.Uname, GiftName: m.Data.GiftName,
		Num: m.Data.Num, Price: m.Data.Price, CoinType: m.Data.CoinType,
	}
}

// parseSuperChatEvent 解析 SUPER_CHAT_MESSAGE（醒目留言）事件。
func parseSuperChatEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID      int64 `json:"uid"`
			UserInfo struct {
				Uname string `json:"uname"`
			} `json:"user_info"`
			Price   int64  `json:"price"`
			Message string `json:"message"`
			Time    int32  `json:"time"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventSuperChat, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.UserInfo.Uname,
		Price: m.Data.Price, Text: m.Data.Message, Duration: m.Data.Time,
	}
}

// parseGuardEvent 解析 GUARD_BUY（舰长/提督/总督购买）事件。
func parseGuardEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID        int64  `json:"uid"`
			Username   string `json:"username"`
			GuardLevel int32  `json:"guard_level"`
			Num        int32  `json:"num"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventGuard, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.Username, Level: m.Data.GuardLevel, Num: m.Data.Num,
	}
}

// parseEntryEffectEvent 解析 ENTRY_EFFECT（高等级用户进场特效）事件。
func parseEntryEffectEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID         int64  `json:"uid"`
			CopyWriting string `json:"copy_writing"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventEntryEffect, Raw: raw,
		UID: m.Data.UID, Text: m.Data.CopyWriting,
	}
}

// toInt64 把 B 站载荷中类型不稳定的数字字段统一转成 int64：
// JSON 数字解析为 float64，部分字段则是数字字符串；其余类型返回 0。
func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}
