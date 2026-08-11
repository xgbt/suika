package data

import (
	"bufio"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/flv"

	"github.com/go-kratos/kratos/v3/log"
)

// Recorder repo defaults (proto zero values are indistinguishable from
// unset, so defaults are applied here; same trick as defaultPageSize).
const (
	defaultRecordRoot     = "./recordings"
	defaultSegmentMinutes = 120
	defaultHealthInterval = 60 * time.Second
	defaultHealthRounds   = 3
	// splitOverrun caps how long a segment may exceed its duration target
	// while waiting for a keyframe to split on.
	splitOverrun = 15 * time.Second
	maxTitleLen  = 64
	maxNameLen   = 32
)

// meta.json status values.
const (
	metaStatusRecording = "recording"
	metaStatusRemuxing  = "remuxing"
	metaStatusDone      = "done"
	metaStatusPartial   = "partial"
)

// segment remux_status values.
const (
	remuxStatusPending = "pending"
	remuxStatusOK      = "ok"
	remuxStatusFailed  = "failed"
)

// sessionMeta is the on-disk session record (PO). The filesystem is the
// source of truth: crash recovery works by scanning meta.json files.
type sessionMeta struct {
	RoomID        int64         `json:"room_id"`
	RoomName      string        `json:"room_name"`
	Title         string        `json:"title"`
	LiveStartTime int64         `json:"live_start_time"`
	EndTime       int64         `json:"end_time"`
	Quality       qualityMeta   `json:"quality"`
	Status        string        `json:"status"`
	Segments      []segmentMeta `json:"segments"`
	Errors        []metaError   `json:"errors"`
	UpdatedAt     int64         `json:"updated_at"`
}

type qualityMeta struct {
	Qn   int32  `json:"qn"`
	Desc string `json:"desc"`
}

type segmentMeta struct {
	Part        int    `json:"part"`
	Video       string `json:"video"`
	FLVKept     bool   `json:"flv_kept"`
	Danmaku     string `json:"danmaku"`
	WallStart   int64  `json:"wall_start"`
	WallEnd     int64  `json:"wall_end"`
	TsStart     int64  `json:"ts_start"`
	TsEnd       int64  `json:"ts_end"`
	Bytes       int64  `json:"bytes"`
	RemuxStatus string `json:"remux_status"`
	RemuxError  string `json:"remux_error,omitempty"`
}

type metaError struct {
	Time  int64  `json:"time"`
	Stage string `json:"stage"`
	Msg   string `json:"msg"`
}

// danmuLine is one JSONL danmaku record (PO).
type danmuLine struct {
	Ts       int64           `json:"ts"`
	Type     string          `json:"type"`
	UID      int64           `json:"uid,omitempty"`
	Uname    string          `json:"uname,omitempty"`
	Text     string          `json:"text,omitempty"`
	Color    int32           `json:"color,omitempty"`
	Mode     int32           `json:"mode,omitempty"`
	GiftName string          `json:"gift_name,omitempty"`
	Num      int32           `json:"num,omitempty"`
	Price    int64           `json:"price,omitempty"`
	CoinType string          `json:"coin_type,omitempty"`
	Duration int32           `json:"duration,omitempty"`
	Level    int32           `json:"level,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
}

type pumpStats struct {
	file  atomic.Value // string
	bytes atomic.Int64
}

// recorderRepo implements biz.RecorderRepo: the recordings directory
// layout, the FLV pump, meta.json bookkeeping, and remux.
type recorderRepo struct {
	d *Data

	recordRoot       string
	segmentDuration  time.Duration // 0 disables splitting
	healthInterval   time.Duration
	healthFailRounds int

	mu    sync.Mutex // guards meta files and the stats map
	stats map[int64]*pumpStats
}

// NewRecorderRepo creates the recorder storage seam.
func NewRecorderRepo(d *Data, c *conf.Recorder) biz.RecorderRepo {
	r := &recorderRepo{
		d:                d,
		recordRoot:       defaultRecordRoot,
		segmentDuration:  defaultSegmentMinutes * time.Minute,
		healthInterval:   defaultHealthInterval,
		healthFailRounds: defaultHealthRounds,
		stats:            make(map[int64]*pumpStats),
	}
	if c == nil {
		return r
	}
	if c.GetRecordRoot() != "" {
		r.recordRoot = c.GetRecordRoot()
	}
	if c.SegmentMinutes != nil {
		r.segmentDuration = time.Duration(c.GetSegmentMinutes()) * time.Minute
	}
	if rc := c.GetReconnect(); rc != nil {
		if rc.GetHealthCheckInterval() != nil {
			r.healthInterval = rc.GetHealthCheckInterval().AsDuration()
		}
		if rc.GetHealthCheckFailRounds() > 0 {
			r.healthFailRounds = int(rc.GetHealthCheckFailRounds())
		}
	}
	return r
}

// NewSessionStatsRepo forwards the narrow stats seam consumed by the room
// API to the exact same recorderRepo instance that backs RecorderRepo: the
// in-flight pump stats state must not be copied or duplicated, otherwise
// the room API would read stale write progress. The assertion holds
// because NewRecorderRepo always returns *recorderRepo, which implements
// both seams.
func NewSessionStatsRepo(repo biz.RecorderRepo) biz.SessionStatsRepo {
	return repo.(biz.SessionStatsRepo)
}

// PrepareSession creates (or re-locates after a restart) the session
// directory and meta.json.
func (r *recorderRepo) PrepareSession(ctx context.Context, session *biz.Session) error {
	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	r.mu.Lock()
	defer r.mu.Unlock()
	if meta, err := loadMeta(metaPath); err == nil {
		// restart resume: same session directory, keep recorded segments.
		meta.Status = metaStatusRecording
		if session.Title != "" {
			meta.Title = session.Title
		}
		if session.RoomName != "" {
			meta.RoomName = session.RoomName
		}
		return saveMeta(metaPath, meta)
	}
	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}
	meta := &sessionMeta{
		RoomID:        session.RoomID,
		RoomName:      session.RoomName,
		Title:         session.Title,
		LiveStartTime: start.Unix(),
		Status:        metaStatusRecording,
	}
	return saveMeta(metaPath, meta)
}

// RecordSession pumps the stream to disk (splitting segments as
// configured) and writes danmaku events to the matching JSONL files.
func (r *recorderRepo) RecordSession(ctx context.Context, session *biz.Session, stream *biz.StreamHandle, events <-chan *biz.DanmakuEvent) (*biz.SessionResult, error) {
	if stream == nil || stream.Body == nil {
		return nil, biz.ErrRoomInternal
	}
	defer stream.Body.Close()

	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	header, err := flv.ParseHeader(stream.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}

	stats := r.statsFor(session.RoomID)
	baseBytes := stats.bytes.Load()
	stats.file.Store("")

	// record the granted stream quality in meta.
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Quality = qualityMeta{Qn: stream.Quality.Qn, Desc: stream.Quality.Desc}
		if session.Title != "" {
			meta.Title = session.Title
		}
	})

	type tagRead struct {
		tag *flv.Tag
		err error
	}
	tagCh := make(chan tagRead, 512)
	go func() {
		for {
			tag, err := flv.ReadTag(stream.Body)
			tagCh <- tagRead{tag, err}
			if err != nil {
				return
			}
		}
	}()

	var (
		cache        headerCache
		seg          *segmentFile
		result       biz.SessionResult
		sessionBytes int64
		lastGrowth   int64
		failRounds   int
	)
	health := time.NewTicker(r.healthInterval)
	defer health.Stop()

	openNewSegment := func() error {
		part := nextPartNumber(dir, base)
		newSeg, err := openSegment(dir, base, part, header, &cache)
		if err != nil {
			return err
		}
		seg = newSeg
		result.Parts++
		stats.file.Store(seg.videoPath)
		r.appendSegmentMeta(metaPath, seg)
		log.Info("segment opened", "room", session.RoomID, "part", part, "file", seg.videoPath)
		return nil
	}
	closeSegment := func() {
		if seg == nil {
			return
		}
		if err := seg.close(); err != nil {
			log.Error("close segment failed", "room", session.RoomID, "file", seg.videoPath, "err", err)
		}
		r.finishSegmentMeta(metaPath, seg)
		seg = nil
	}

	for {
		select {
		case <-ctx.Done():
			closeSegment()
			return &result, ctx.Err()
		case tr := <-tagCh:
			if tr.err != nil {
				closeSegment()
				if tr.err == io.EOF {
					return &result, nil
				}
				return &result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.err)
			}
			tag := tr.tag
			if seg == nil {
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			} else if r.shouldSplit(seg, tag) {
				closeSegment()
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			}
			// Cache header tags only after the open/split decision: the tag
			// that triggered the new segment must not be re-injected from the
			// cache, or it would be written twice (once by openSegment, once
			// by the pump below). Headers seen before the split are still
			// re-injected in full.
			switch {
			case tag.IsMetadata():
				cache.metadata = tag
			case tag.IsAVCSequenceHeader():
				cache.videoSeq = tag
			case tag.IsAACSequenceHeader():
				cache.audioSeq = tag
			}
			n, err := seg.writeTag(tag)
			sessionBytes += n
			result.BytesWritten = sessionBytes
			stats.bytes.Store(baseBytes + sessionBytes)
			if err != nil {
				closeSegment()
				r.appendMetaError(metaPath, "record", err)
				return &result, err
			}
		case ev := <-events:
			if seg == nil {
				continue
			}
			if err := seg.writeEvent(ev); err != nil {
				log.Warn("danmaku write failed", "room", session.RoomID, "err", err)
			}
		case <-health.C:
			if sessionBytes > lastGrowth {
				lastGrowth = sessionBytes
				failRounds = 0
				continue
			}
			failRounds++
			if failRounds >= r.healthFailRounds {
				closeSegment()
				return &result, fmt.Errorf("recording unhealthy: no new data for %d rounds", failRounds)
			}
		}
	}
}

// shouldSplit decides whether the next tag should open a new segment: the
// duration target is reached and the tag is a keyframe, or the overrun
// budget is exhausted (force split).
func (r *recorderRepo) shouldSplit(seg *segmentFile, tag *flv.Tag) bool {
	if r.segmentDuration <= 0 || !seg.hasStart {
		return false
	}
	elapsed := time.Duration(tag.Timestamp-seg.startTs) * time.Millisecond
	if elapsed < r.segmentDuration {
		return false
	}
	return tag.IsVideoKeyframe() || elapsed >= r.segmentDuration+splitOverrun
}

// FinishSession finalizes meta.json and remuxes all recorded segments.
func (r *recorderRepo) FinishSession(ctx context.Context, session *biz.Session) error {
	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	r.mu.Lock()
	meta, err := loadMeta(metaPath)
	if err != nil {
		r.mu.Unlock()
		if os.IsNotExist(err) {
			return nil // nothing was recorded
		}
		return err
	}
	meta.Status = metaStatusRemuxing
	meta.EndTime = time.Now().Unix()
	if session.Title != "" {
		meta.Title = session.Title
	}
	meta.Quality = qualityMeta{Qn: session.Quality.Qn, Desc: session.Quality.Desc}
	err = saveMeta(metaPath, meta)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.finalizeSegments(ctx, metaPath, meta)
}

// finalizeSegments remuxes every pending segment one by one, persisting
// meta.json after each so progress survives crashes. Remux failures keep
// the FLV and are recorded in meta; they never delete data.
func (r *recorderRepo) finalizeSegments(ctx context.Context, metaPath string, meta *sessionMeta) error {
	dir := filepath.Dir(metaPath)
	allOK := true
	for i := range meta.Segments {
		seg := &meta.Segments[i]
		if seg.RemuxStatus == remuxStatusOK {
			continue
		}
		if !r.d.remuxEnabled {
			seg.RemuxStatus = remuxStatusOK
			seg.FLVKept = true
			r.persistMeta(metaPath, meta)
			continue
		}
		flvPath := filepath.Join(dir, seg.Video)
		mp4Name := strings.TrimSuffix(seg.Video, ".flv") + ".mp4"
		mp4Path := filepath.Join(dir, mp4Name)

		if _, err := os.Stat(flvPath); err != nil {
			if _, merr := os.Stat(mp4Path); merr == nil {
				seg.RemuxStatus = remuxStatusOK
				seg.Video = mp4Name
				seg.FLVKept = false
			} else {
				seg.RemuxStatus = remuxStatusFailed
				seg.RemuxError = "source flv missing"
				allOK = false
			}
			r.persistMeta(metaPath, meta)
			continue
		}

		if err := remuxWithRetry(ctx, r.d.ffmpegPath, flvPath, mp4Path, meta.Title, meta.RoomName, meta.LiveStartTime); err != nil {
			seg.RemuxStatus = remuxStatusFailed
			seg.RemuxError = err.Error()
			seg.FLVKept = true
			allOK = false
			log.Error("remux failed, keeping flv", "file", flvPath, "err", err)
		} else if fi, serr := os.Stat(mp4Path); serr != nil || fi.Size() == 0 {
			// never delete a source whose replacement is not verified
			seg.RemuxStatus = remuxStatusFailed
			seg.RemuxError = "remux output missing or empty"
			seg.FLVKept = true
			allOK = false
		} else {
			_ = os.Remove(flvPath)
			seg.RemuxStatus = remuxStatusOK
			seg.Video = mp4Name
			seg.FLVKept = false
		}
		r.persistMeta(metaPath, meta)
	}
	if allOK {
		meta.Status = metaStatusDone
	} else {
		meta.Status = metaStatusPartial
	}
	return r.persistMeta(metaPath, meta)
}

// RecoverPending finishes remux work left over from a previous run by
// scanning every meta.json under the record root.
func (r *recorderRepo) RecoverPending(ctx context.Context) error {
	pattern := filepath.Join(r.recordRoot, "*", "*", "*.meta.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.mu.Lock()
		meta, err := loadMeta(path)
		r.mu.Unlock()
		if err != nil {
			log.Warn("recover: unreadable meta.json", "path", path, "err", err)
			continue
		}
		switch meta.Status {
		case metaStatusRemuxing, metaStatusRecording:
			log.Info("recovering unfinished session", "path", path, "status", meta.Status)
			meta.Status = metaStatusRemuxing
			if meta.EndTime == 0 {
				meta.EndTime = time.Now().Unix()
			}
			r.persistMeta(path, meta)
			if err := r.finalizeSegments(ctx, path, meta); err != nil {
				log.Warn("recover: finalize failed", "path", path, "err", err)
			}
		case metaStatusPartial, metaStatusDone:
			if hasRetryableSegments(meta) {
				log.Info("retrying failed segments", "path", path)
				if err := r.finalizeSegments(ctx, path, meta); err != nil {
					log.Warn("recover: finalize failed", "path", path, "err", err)
				}
			}
		}
	}
	return nil
}

func hasRetryableSegments(meta *sessionMeta) bool {
	for _, seg := range meta.Segments {
		if seg.RemuxStatus == remuxStatusFailed && seg.FLVKept {
			return true
		}
	}
	return false
}

// SessionStats reports in-flight write progress for a room's session.
func (r *recorderRepo) SessionStats(_ context.Context, roomID int64) (*biz.SessionStats, error) {
	r.mu.Lock()
	s, ok := r.stats[roomID]
	r.mu.Unlock()
	if !ok {
		return nil, nil
	}
	file, _ := s.file.Load().(string)
	return &biz.SessionStats{CurrentFile: file, BytesWritten: s.bytes.Load()}, nil
}

// --- helpers ---

func (r *recorderRepo) statsFor(roomID int64) *pumpStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.stats[roomID]
	if !ok {
		s = &pumpStats{}
		r.stats[roomID] = s
	}
	return s
}

// sessionPaths computes the session directory and the file-name base
// (date/time/title prefix shared by all parts and the meta file).
func (r *recorderRepo) sessionPaths(session *biz.Session) (dir string, base string, err error) {
	if session == nil || session.RoomID <= 0 {
		return "", "", biz.ErrRoomInternal
	}
	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}
	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.RoomName, maxNameLen))
	dir = filepath.Join(r.recordRoot, roomDir, start.Format("2006-01-02"))
	base = start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title, maxTitleLen)
	return dir, base, nil
}

// sanitizeSegment replaces unsafe characters (\/:*?"<>|, control chars)
// and whitespace runs with single underscores, truncates to max runes, and
// falls back to "untitled".
func sanitizeSegment(s string, max int) string {
	var sb strings.Builder
	prevUnderscore := false
	for _, r := range s {
		var out rune
		switch {
		case r < 0x20 || r == 0x7f:
			out = '_'
		case strings.ContainsRune(`\/:*?"<>|`, r):
			out = '_'
		case unicode.IsSpace(r):
			out = '_'
		default:
			out = r
		}
		if out == '_' && prevUnderscore {
			continue
		}
		sb.WriteRune(out)
		prevUnderscore = out == '_'
	}
	cleaned := strings.Trim(sb.String(), "_")
	runes := []rune(cleaned)
	if len(runes) > max {
		cleaned = string(runes[:max])
		cleaned = strings.TrimRight(cleaned, "_")
	}
	if cleaned == "" {
		return "untitled"
	}
	return cleaned
}

var partSuffixPattern = regexp.MustCompile(`_part(\d+)\.(flv|mp4)$`)

// nextPartNumber derives the next part number by scanning the session
// directory (covers both reconnects and crash restarts).
func nextPartNumber(dir, base string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	maxPart := 0
	prefix := base + "_part"
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		m := partSuffixPattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && n > maxPart {
			maxPart = n
		}
	}
	return maxPart + 1
}

func loadMeta(path string) (*sessionMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta sessionMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func saveMeta(path string, meta *sessionMeta) error {
	meta.UpdatedAt = time.Now().Unix()
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// updateMeta applies fn to the meta file under the repo lock.
func (r *recorderRepo) updateMeta(metaPath string, fn func(*sessionMeta)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, err := loadMeta(metaPath)
	if err != nil {
		return
	}
	fn(meta)
	if err := saveMeta(metaPath, meta); err != nil {
		log.Error("save meta failed", "path", metaPath, "err", err)
	}
}

func (r *recorderRepo) persistMeta(metaPath string, meta *sessionMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return saveMeta(metaPath, meta)
}

func (r *recorderRepo) appendSegmentMeta(metaPath string, seg *segmentFile) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Segments = append(meta.Segments, segmentMeta{
			Part:        seg.part,
			Video:       filepath.Base(seg.videoPath),
			Danmaku:     filepath.Base(seg.danmuPath),
			WallStart:   seg.wallStart.Unix(),
			RemuxStatus: remuxStatusPending,
		})
	})
}

func (r *recorderRepo) finishSegmentMeta(metaPath string, seg *segmentFile) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		for i := range meta.Segments {
			s := &meta.Segments[i]
			if s.Part != seg.part {
				continue
			}
			s.WallEnd = time.Now().Unix()
			s.TsStart = seg.startTs
			s.TsEnd = seg.lastTs
			s.Bytes = seg.bytes
		}
	})
}

func (r *recorderRepo) appendMetaError(metaPath, stage string, err error) {
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Errors = append(meta.Errors, metaError{Time: time.Now().Unix(), Stage: stage, Msg: err.Error()})
	})
}

// --- segment files ---

// headerCache holds the tags re-injected at the start of every segment.
type headerCache struct {
	metadata *flv.Tag
	videoSeq *flv.Tag
	audioSeq *flv.Tag
}

// segmentFile is one part: an FLV file plus its danmaku JSONL.
type segmentFile struct {
	part      int
	videoPath string
	danmuPath string
	vf        *os.File
	df        *os.File
	bw        *bufio.Writer
	hasStart  bool
	startTs   int64 // stream timestamp of the first tag, ms
	lastTs    int64
	bytes     int64
	wallStart time.Time
}

// openSegment starts a new part: FLV header plus the cached onMetaData /
// sequence-header tags, so the part is independently playable.
func openSegment(dir, base string, part int, header *flv.FileHeader, cache *headerCache) (*segmentFile, error) {
	videoPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.flv", base, part))
	danmuPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.danmu.jsonl", base, part))
	vf, err := os.OpenFile(videoPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	df, err := os.OpenFile(danmuPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		vf.Close()
		return nil, err
	}
	seg := &segmentFile{
		part: part, videoPath: videoPath, danmuPath: danmuPath,
		vf: vf, df: df,
		bw:        bufio.NewWriterSize(vf, 1<<20),
		wallStart: time.Now(),
	}
	hb := header.Bytes()
	if _, err := seg.bw.Write(hb); err != nil {
		seg.close()
		return nil, err
	}
	seg.bytes += int64(len(hb))
	for _, tag := range []*flv.Tag{cache.metadata, cache.videoSeq, cache.audioSeq} {
		if tag == nil {
			continue
		}
		tb := tag.AppendTo(nil)
		if _, err := seg.bw.Write(tb); err != nil {
			seg.close()
			return nil, err
		}
		seg.bytes += int64(len(tb))
	}
	return seg, nil
}

func (s *segmentFile) writeTag(tag *flv.Tag) (int64, error) {
	buf := tag.AppendTo(nil)
	n, err := s.bw.Write(buf)
	if n > 0 {
		s.bytes += int64(n)
		if !s.hasStart {
			s.hasStart = true
			s.startTs = tag.Timestamp
		}
		s.lastTs = tag.Timestamp
	}
	return int64(n), err
}

func (s *segmentFile) writeEvent(ev *biz.DanmakuEvent) error {
	line := danmuLine{
		Ts:       ev.Ts.UnixMilli(),
		Type:     ev.Type,
		UID:      ev.UID,
		Uname:    ev.Uname,
		Text:     ev.Text,
		Color:    ev.Color,
		Mode:     ev.Mode,
		GiftName: ev.GiftName,
		Num:      ev.Num,
		Price:    ev.Price,
		CoinType: ev.CoinType,
		Duration: ev.Duration,
		Level:    ev.Level,
		Raw:      ev.Raw,
	}
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	_, err = s.df.Write(append(data, '\n'))
	return err
}

func (s *segmentFile) close() error {
	err := s.bw.Flush()
	return stderrors.Join(err, s.vf.Close(), s.df.Close())
}
