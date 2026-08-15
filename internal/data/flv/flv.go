// Package flv 实现录制直播流所需的最小 FLV 解析与写入：直接落盘，
// 并在 tag 边界处切分。
package flv

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FLV tag 类型。
const (
	TagAudio  byte = 8
	TagVideo  byte = 9
	TagScript byte = 18
)

const (
	// HeaderSize 是 FLV 文件头的字节数。
	HeaderSize     = 9
	tagHeaderSize  = 11
	prevTagSizeLen = 4
)

// FileHeader 是解析后的 FLV 文件头。
type FileHeader struct {
	Version  byte
	HasAudio bool
	HasVideo bool
}

// ParseHeader 读取 9 字节 FLV 头及紧随其后的 PreviousTagSize0。
func ParseHeader(r io.Reader) (*FileHeader, error) {
	var buf [HeaderSize + prevTagSizeLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("flv: read header: %w", err)
	}
	if buf[0] != 'F' || buf[1] != 'L' || buf[2] != 'V' {
		return nil, fmt.Errorf("flv: bad signature %q", buf[0:3])
	}
	return &FileHeader{
		Version:  buf[3],
		HasAudio: buf[4]&0x04 != 0,
		HasVideo: buf[4]&0x01 != 0,
	}, nil
}

// Bytes 渲染文件头及紧随其后的零值 PreviousTagSize0。
func (h *FileHeader) Bytes() []byte {
	buf := make([]byte, HeaderSize+prevTagSizeLen)
	buf[0], buf[1], buf[2] = 'F', 'L', 'V'
	buf[3] = h.Version
	var flags byte
	if h.HasAudio {
		flags |= 0x04
	}
	if h.HasVideo {
		flags |= 0x01
	}
	buf[4] = flags
	binary.BigEndian.PutUint32(buf[5:9], HeaderSize)
	// PreviousTagSize0 保持为零。
	return buf
}

// Tag 是一个 FLV tag，时间戳已解码。
type Tag struct {
	Type      byte
	Timestamp int64 // 毫秒，含扩展字节
	Data      []byte
}

// ReadTag 读取一个 tag（11 字节头、载荷、尾部 PreviousTagSize）。
// 仅当流在读到任何头字节之前干净结束时才返回 io.EOF。
func ReadTag(r io.Reader) (*Tag, error) {
	var head [tagHeaderSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		return nil, err
	}
	dataSize := int(head[1])<<16 | int(head[2])<<8 | int(head[3])
	ts := int64(head[7])<<24 | int64(head[4])<<16 | int64(head[5])<<8 | int64(head[6])

	data := make([]byte, dataSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("flv: read tag data: %w", err)
	}
	var prev [prevTagSizeLen]byte
	if _, err := io.ReadFull(r, prev[:]); err != nil {
		return nil, fmt.Errorf("flv: read previous tag size: %w", err)
	}
	return &Tag{Type: head[0], Timestamp: ts, Data: data}, nil
}

// AppendTo 把 tag（头、载荷、PreviousTagSize）序列化后追加到 b。
func (t *Tag) AppendTo(b []byte) []byte {
	size := len(t.Data)
	head := [tagHeaderSize]byte{
		0: t.Type,
		1: byte(size >> 16),
		2: byte(size >> 8),
		3: byte(size),
		4: byte(t.Timestamp >> 16),
		5: byte(t.Timestamp >> 8),
		6: byte(t.Timestamp),
		7: byte(t.Timestamp >> 24),
	}
	// streamID 保持为零（字节 8..10）
	b = append(b, head[:]...)
	b = append(b, t.Data...)
	var prev [prevTagSizeLen]byte
	binary.BigEndian.PutUint32(prev[:], uint32(tagHeaderSize+size))
	return append(b, prev[:]...)
}

// IsMetadata 判断 tag 是否为 onMetaData 脚本标签。
func (t *Tag) IsMetadata() bool {
	return t.Type == TagScript
}

// IsVideoKeyframe 判断 tag 是否携带关键帧画面（frameType 1 且载荷为
// NALU，即非序列头）。用于选择安全的切分点。
func (t *Tag) IsVideoKeyframe() bool {
	return t.Type == TagVideo && len(t.Data) > 1 && t.Data[0]>>4 == 1 && t.Data[1] == 1
}

// IsAVCSequenceHeader 判断 tag 是否携带 AVC 解码器配置记录
// （AVCPacketType 0）。
func (t *Tag) IsAVCSequenceHeader() bool {
	return t.Type == TagVideo && len(t.Data) > 1 && t.Data[1] == 0
}

// IsAACSequenceHeader 判断 tag 是否携带 AAC AudioSpecificConfig
// （SoundFormat 10、AACPacketType 0）。
func (t *Tag) IsAACSequenceHeader() bool {
	return t.Type == TagAudio && len(t.Data) > 1 && t.Data[0]>>4 == 0xA && t.Data[1] == 0
}
