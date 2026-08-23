package bili

import (
	"strings"
	"testing"
)

func flvCodec(codecName, baseURL string, hosts ...hostURL) codecLine {
	return codecLine{CodecName: codecName, BaseURL: baseURL, URLInfo: hosts}
}

func TestPickFLVStreamPrefersAVC(t *testing.T) {
	pu := playURL{
		CurrentQn: 10000,
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("hevc", "/live/hevc.flv", hostURL{Host: "https://cdn-1", Extra: "?sig=hevc"}),
			flvCodec("avc", "/live/avc.flv", hostURL{Host: "https://cdn-2", Extra: "?sig=avc"}),
		}}}}},
	}

	url, quality, err := pickFLVStream(pu, 10000, 1)
	if err != nil {
		t.Fatalf("pickFLVStream: %v", err)
	}
	if url != "https://cdn-2/live/avc.flv?sig=avc" {
		t.Fatalf("url = %q, want the AVC candidate", url)
	}
	if quality.Qn != 10000 {
		t.Fatalf("quality.Qn = %d, want 10000", quality.Qn)
	}
}

func TestPickFLVStreamFirstAVCWinsAmongEquals(t *testing.T) {
	pu := playURL{
		CurrentQn: 150,
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/a.flv", hostURL{Host: "https://cdn-1"}),
			flvCodec("avc", "/live/b.flv", hostURL{Host: "https://cdn-2"}),
		}}}}},
	}

	url, _, err := pickFLVStream(pu, 150, 1)
	if err != nil {
		t.Fatalf("pickFLVStream: %v", err)
	}
	if url != "https://cdn-1/live/a.flv" {
		t.Fatalf("url = %q, want the first equal-priority candidate", url)
	}
}

func TestPickFLVStreamSkipsNonFLV(t *testing.T) {
	pu := playURL{
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/index.m3u8", hostURL{Host: "https://cdn-1"}),
		}}}}},
	}

	_, _, err := pickFLVStream(pu, 10000, 1)
	if err == nil || !strings.Contains(err.Error(), "no FLV stream candidate") {
		t.Fatalf("err = %v, want no-candidate error", err)
	}
}

func TestPickFLVStreamSkipsEmptyHostOrBaseURL(t *testing.T) {
	pu := playURL{
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/a.flv", hostURL{Host: ""}),
			flvCodec("avc", "", hostURL{Host: "https://cdn-1"}),
		}}}}},
	}

	_, _, err := pickFLVStream(pu, 10000, 1)
	if err == nil {
		t.Fatal("want no-candidate error when every host or base url is empty")
	}
}

func TestPickFLVStreamQualityFromGQnDesc(t *testing.T) {
	pu := playURL{
		CurrentQn: 400,
		GQnDesc:   []qnDesc{{Qn: 400, Desc: "蓝光"}, {Qn: 150, Desc: "高清"}},
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/a.flv", hostURL{Host: "https://cdn-1"}),
		}}}}},
	}

	_, quality, err := pickFLVStream(pu, 10000, 1)
	if err != nil {
		t.Fatalf("pickFLVStream: %v", err)
	}
	if quality.Qn != 400 || quality.Desc != "蓝光" {
		t.Fatalf("quality = %+v, want {400 蓝光}", quality)
	}
}

func TestPickFLVStreamQualityFallbackToRequested(t *testing.T) {
	pu := playURL{
		CurrentQn: 0,
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/a.flv", hostURL{Host: "https://cdn-1"}),
		}}}}},
	}

	_, quality, err := pickFLVStream(pu, 10000, 1)
	if err != nil {
		t.Fatalf("pickFLVStream: %v", err)
	}
	if quality.Qn != 10000 || quality.Desc != "原画" {
		t.Fatalf("quality = %+v, want {10000 原画} via qnNames fallback", quality)
	}
}

func TestPickFLVStreamAcceptsDowngrade(t *testing.T) {
	pu := playURL{
		CurrentQn: 150,
		Stream: []streamLine{{Format: []formatLine{{Codec: []codecLine{
			flvCodec("avc", "/live/a.flv", hostURL{Host: "https://cdn-1"}),
		}}}}},
	}

	url, quality, err := pickFLVStream(pu, 10000, 1)
	if err != nil {
		t.Fatalf("pickFLVStream: %v", err)
	}
	if url == "" || quality.Qn != 150 {
		t.Fatalf("got (%q, %+v), want the granted 150 despite requesting 10000", url, quality)
	}
}
