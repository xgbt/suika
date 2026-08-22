package flv

import (
	"bytes"
	"io"
	"testing"
)

func buildTag(typ byte, ts int64, data []byte) []byte {
	return (&Tag{Type: typ, Timestamp: ts, Data: data}).AppendTo(nil)
}

func TestHeaderRoundTrip(t *testing.T) {
	h := &FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	parsed, err := ParseHeader(bytes.NewReader(h.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if *parsed != *h {
		t.Fatalf("parsed = %+v, want %+v", parsed, h)
	}
}

func TestParseHeaderBadSignature(t *testing.T) {
	if _, err := ParseHeader(bytes.NewReader([]byte("FLX\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00"))); err == nil {
		t.Fatal("want error for bad signature")
	}
}

func TestReadTagStream(t *testing.T) {
	h := &FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	var buf bytes.Buffer
	buf.Write(h.Bytes())
	script := []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}
	avcSeq := []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}
	aacSeq := []byte{0xAF, 0x00, 0x12, 0x10}
	key := []byte{0x17, 0x01, 0, 0, 0, 0xAA}
	inter := []byte{0x27, 0x01, 0, 0, 0, 0xBB}
	audio := []byte{0xAF, 0x01, 0x09}

	buf.Write(buildTag(TagScript, 0, script))
	buf.Write(buildTag(TagVideo, 0, avcSeq))
	buf.Write(buildTag(TagAudio, 0, aacSeq))
	buf.Write(buildTag(TagVideo, 1000, key))
	buf.Write(buildTag(TagVideo, 1040, inter))
	buf.Write(buildTag(TagAudio, 7199840, audio))
	// 时间戳扩展字节：0x01_000005
	buf.Write(buildTag(TagVideo, 16777221, key))

	r := bytes.NewReader(buf.Bytes())
	if _, err := ParseHeader(r); err != nil {
		t.Fatal(err)
	}

	tag, err := ReadTag(r)
	if err != nil || !tag.IsMetadata() || !bytes.Equal(tag.Data, script) {
		t.Fatalf("script tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || !tag.IsAVCSequenceHeader() || tag.IsVideoKeyframe() {
		t.Fatalf("avc seq tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || !tag.IsAACSequenceHeader() {
		t.Fatalf("aac seq tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || !tag.IsVideoKeyframe() || tag.Timestamp != 1000 {
		t.Fatalf("keyframe tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || tag.IsVideoKeyframe() || tag.Timestamp != 1040 {
		t.Fatalf("inter tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || tag.Timestamp != 7199840 {
		t.Fatalf("audio tag = %+v, %v", tag, err)
	}
	tag, err = ReadTag(r)
	if err != nil || tag.Timestamp != 16777221 {
		t.Fatalf("extended-ts tag = %+v (ts=%d), %v", tag, tag.Timestamp, err)
	}
	if _, err := ReadTag(r); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestReadTagTruncated(t *testing.T) {
	full := buildTag(TagVideo, 10, []byte{0x17, 0x01, 0, 0, 0})
	// 在载荷中间截断：必须报错，而不是干净的 EOF
	if _, err := ReadTag(bytes.NewReader(full[:len(full)-3])); err == nil {
		t.Fatal("want error for truncated tag")
	}
}
