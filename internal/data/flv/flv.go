// Package flv implements the minimal FLV parsing and writing needed to
// record a live stream directly to disk and split it at tag boundaries.
package flv

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FLV tag types.
const (
	TagAudio  byte = 8
	TagVideo  byte = 9
	TagScript byte = 18
)

const (
	// HeaderSize is the FLV file header size in bytes.
	HeaderSize     = 9
	tagHeaderSize  = 11
	prevTagSizeLen = 4
)

// FileHeader is a parsed FLV file header.
type FileHeader struct {
	Version  byte
	HasAudio bool
	HasVideo bool
}

// ParseHeader reads the 9-byte FLV header plus the first PreviousTagSize0.
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

// Bytes renders the header plus the zero PreviousTagSize0 that follows it.
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
	// PreviousTagSize0 stays zero.
	return buf
}

// Tag is one FLV tag with its decoded timestamp.
type Tag struct {
	Type      byte
	Timestamp int64 // milliseconds, extension byte included
	Data      []byte
}

// ReadTag reads one tag (11-byte header, payload, trailing PreviousTagSize).
// Returns io.EOF only when the stream ends cleanly before any header byte.
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

// AppendTo serializes the tag (header, payload, PreviousTagSize) into b.
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
	// streamID stays zero (bytes 8..10)
	b = append(b, head[:]...)
	b = append(b, t.Data...)
	var prev [prevTagSizeLen]byte
	binary.BigEndian.PutUint32(prev[:], uint32(tagHeaderSize+size))
	return append(b, prev[:]...)
}

// IsMetadata reports whether the tag is an onMetaData script tag.
func (t *Tag) IsMetadata() bool {
	return t.Type == TagScript
}

// IsVideoKeyframe reports whether the tag carries a keyframe picture
// (frameType 1 with a NALU payload, i.e. not a sequence header). Used to
// pick safe split points.
func (t *Tag) IsVideoKeyframe() bool {
	return t.Type == TagVideo && len(t.Data) > 1 && t.Data[0]>>4 == 1 && t.Data[1] == 1
}

// IsAVCSequenceHeader reports whether the tag carries the AVC decoder
// configuration record (AVCPacketType 0).
func (t *Tag) IsAVCSequenceHeader() bool {
	return t.Type == TagVideo && len(t.Data) > 1 && t.Data[1] == 0
}

// IsAACSequenceHeader reports whether the tag carries the AAC
// AudioSpecificConfig (SoundFormat 10, AACPacketType 0).
func (t *Tag) IsAACSequenceHeader() bool {
	return t.Type == TagAudio && len(t.Data) > 1 && t.Data[0]>>4 == 0xA && t.Data[1] == 0
}
