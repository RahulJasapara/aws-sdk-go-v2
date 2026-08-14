package transfermanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// concurrentReader downloads an object's parts across a fixed pool of worker
// goroutines and exposes them to the caller as a single in-order io.Reader.
//
// Unlike a per-Read worker pool, the workers and the dispatcher are spawned
// exactly once (lazily, on the first Read) and live for the whole download.
// A sliding in-flight window bounds both parallelism and buffered memory: at
// most window parts are outstanding at any time, so buffered bytes never
// exceed window*partSize (<= GetObjectBufferSize). The window advances one
// part at a time as the caller consumes data, so in-flight requests do not
// collapse to zero between batches.
type concurrentReader struct {
	options Options
	in      *GetObjectInput

	partSize   int64
	partsCount int32
	// window is the maximum number of parts in flight at once. It equals
	// clamp(1, GetObjectBufferSize/partSize, Concurrency) and caps the sem.
	window int32
	// totalBytes is the absolute end offset (exclusive) of the requested
	// range, used to compute per-part Range headers for range downloads.
	totalBytes int64
	// pos is the absolute start offset of the download (0, or the range start).
	pos  int64
	etag *string

	// ch carries completed chunks from workers to the consumer. Its capacity
	// is >= window so a worker's send never blocks against a full buffer while
	// permits remain (anti-deadlock invariant).
	ch chan outChunk
	// work carries part requests from the dispatcher to the workers.
	work chan getChunk
	// sem is the sliding-window permit channel (capacity window). The
	// dispatcher acquires a permit before scheduling a part; the consumer
	// releases it once that part is fully delivered.
	sem chan struct{}
	// done is closed exactly once on teardown to unblock every goroutine.
	done chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	// buf parks out-of-order chunks until the consumer's cursor reaches them.
	buf       map[int32]*outChunk
	nextIndex int32
	delivered int32

	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	err atomic.Pointer[error]
}

// getWindow returns the sliding in-flight window width, clamped to
// [1, Concurrency] and never exceeding GetObjectBufferSize/partSize so that
// buffered bytes stay within the configured memory cap.
func getWindow(concurrency int, bufferSize, partSize int64) int32 {
	if concurrency < 1 {
		concurrency = 1
	}
	if partSize < 1 {
		partSize = 1
	}
	sections := bufferSize / partSize
	if sections < 1 {
		sections = 1
	}
	w := int64(concurrency)
	if sections < w {
		w = sections
	}
	if w < 1 {
		w = 1
	}
	return int32(w)
}

// Read implements io.Reader, composing object parts in order into p. It blocks
// only when nothing has been produced yet and parts remain; otherwise it
// returns whatever contiguous bytes are already available.
func (r *concurrentReader) Read(p []byte) (int, error) {
	r.start()

	if len(p) == 0 {
		return 0, nil
	}

	var written int
	for written < len(p) {
		c, ok := r.buf[r.nextIndex]
		if !ok {
			if r.delivered >= r.partsCount {
				r.teardown()
				return written, io.EOF
			}
			if written > 0 {
				// Return the contiguous bytes we already have rather than
				// blocking for the next part.
				return written, nil
			}
			// Nothing buffered yet and the caller wants data: wait for the
			// next chunk to arrive, or for teardown.
			select {
			case <-r.done:
				return written, r.terminalErr()
			case oc := <-r.ch:
				r.buf[oc.index] = &oc
			}
			continue
		}

		n, _ := c.body.Read(p[written:])
		c.cur += int64(n)
		written += n

		if c.cur >= c.length {
			delete(r.buf, r.nextIndex)
			r.nextIndex++
			r.delivered++
			r.releasePermit()
			if r.delivered >= r.partsCount {
				r.teardown()
				return written, io.EOF
			}
		}
	}

	return written, nil
}

// Close aborts an in-progress download, releasing worker goroutines and
// canceling any in-flight requests. It is safe to call multiple times.
func (r *concurrentReader) Close() error {
	r.teardown()
	return nil
}

// initReader wires up the cancelable context and done channel. It must run
// before start or Close so teardown is always safe, even for a metadata-only
// caller that closes without reading.
func (r *concurrentReader) initReader(ctx context.Context) {
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan struct{})
	if r.buf == nil {
		r.buf = make(map[int32]*outChunk)
	}
}

// start lazily spawns the worker pool and dispatcher exactly once, so
// metadata-only callers that never Read spawn nothing.
func (r *concurrentReader) start() {
	r.startOnce.Do(func() {
		// Already torn down (e.g. Close before the first Read): spawn nothing.
		select {
		case <-r.done:
			return
		default:
		}

		workers := r.options.Concurrency
		if workers < 1 {
			workers = 1
		}
		r.ch = make(chan outChunk, workers)
		r.work = make(chan getChunk)
		window := r.window
		if window < 1 {
			window = 1
		}
		r.sem = make(chan struct{}, window)

		clientOptions := []func(*s3.Options){
			func(o *s3.Options) {
				o.APIOptions = append(o.APIOptions,
					middleware.AddSDKAgentKey(middleware.FeatureMetadata, userAgentKey),
					addFeatureUserAgent,
				)
			}}

		for i := 0; i < workers; i++ {
			r.wg.Add(1)
			go r.worker(clientOptions...)
		}
		r.wg.Add(1)
		go r.dispatch()
		r.wg.Add(1)
		go r.watch()
	})
}

// watch bridges context cancellation to teardown. Workers and the dispatcher
// only ever wait on done, so if the caller cancels the context while the window
// is saturated and no request is in flight to observe the cancellation, nothing
// would otherwise unblock them. This closes done (recording ctx.Err) so they
// exit promptly. It exits cleanly when done is closed by any other path.
func (r *concurrentReader) watch() {
	defer r.wg.Done()
	select {
	case <-r.ctx.Done():
		r.fail(r.ctx.Err())
	case <-r.done:
	}
}

// dispatch is the single persistent scheduler: for each part it acquires a
// window permit (blocking while the window is full) and hands the request to
// a worker. Every blocking operation also selects on done for prompt teardown.
func (r *concurrentReader) dispatch() {
	defer r.wg.Done()

	pos := r.pos
	for i := int32(0); i < r.partsCount; i++ {
		select {
		case <-r.done:
			return
		case r.sem <- struct{}{}:
		}

		var gc getChunk
		if r.options.GetObjectType == types.GetObjectParts {
			gc = getChunk{part: i + 1, index: i}
		} else {
			gc = getChunk{withRange: r.byteRange(pos), index: i}
		}
		pos += r.partSize

		select {
		case <-r.done:
			return
		case r.work <- gc:
		}
	}
	close(r.work)
}

// worker pulls part requests off work until it is drained and closed or the
// download is torn down.
func (r *concurrentReader) worker(clientOptions ...func(*s3.Options)) {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			return
		case chunk, ok := <-r.work:
			if !ok {
				return
			}
			r.downloadChunk(chunk, clientOptions...)
		}
	}
}

// downloadChunk fetches a single part, retrying transient body-read failures,
// and hands the buffered bytes to the consumer.
func (r *concurrentReader) downloadChunk(chunk getChunk, clientOptions ...func(*s3.Options)) {
	params := r.in.mapGetObjectInput(!r.options.DisableChecksumValidation)
	if chunk.part != 0 {
		params.PartNumber = aws.Int32(chunk.part)
	}
	if chunk.withRange != "" {
		params.Range = aws.String(chunk.withRange)
	}
	if params.VersionId == nil {
		params.IfMatch = r.etag
	}

	// Always make at least one attempt; PartBodyMaxRetries is the retry budget.
	attempts := r.options.PartBodyMaxRetries
	if attempts < 1 {
		attempts = 1
	}

	var buf []byte
	var err error
	for retry := 0; retry < attempts; retry++ {
		buf, err = r.tryDownloadChunk(params, clientOptions...)
		if err == nil {
			break
		}
		// Only an error reading the response body is retryable; anything else
		// (request failure, range mismatch) is terminal.
		if _, ok := err.(*errReadingBody); !ok {
			r.fail(err)
			return
		}
	}
	if err != nil {
		r.fail(err)
		return
	}

	select {
	case <-r.done:
	case r.ch <- outChunk{body: bytes.NewReader(buf), index: chunk.index, length: int64(len(buf))}:
	}
}

// tryDownloadChunk performs one GetObject attempt and reads the full part body
// into a sized buffer. A failure while reading the body is wrapped as
// *errReadingBody so downloadChunk can retry it.
func (r *concurrentReader) tryDownloadChunk(params *s3.GetObjectInput, clientOptions ...func(*s3.Options)) ([]byte, error) {
	out, err := r.options.S3.GetObject(r.ctx, params, clientOptions...)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	if params.Range != nil && out.ContentRange != nil {
		reqStart, reqEnd, err := getReqRange(aws.ToString(params.Range))
		if err != nil {
			return nil, err
		}
		respStart, respEnd, err := getRespRange(aws.ToString(out.ContentRange))
		if err != nil {
			return nil, err
		}
		// don't validate first chunk since object size is unknown when getting that
		if reqStart != 0 && (reqStart != respStart || reqEnd != respEnd) {
			return nil, fmt.Errorf("range mismatch between request %d-%d and response %d-%d", reqStart, reqEnd, respStart, respEnd)
		}
	}

	// Length is known from ContentLength, so read into an exactly-sized buffer
	// instead of letting io.ReadAll grow one.
	buf := make([]byte, aws.ToInt64(out.ContentLength))
	if _, err := io.ReadFull(out.Body, buf); err != nil {
		return nil, &errReadingBody{err: err}
	}
	return buf, nil
}

// byteRange returns an HTTP Byte-Range header value for the part starting at pos.
func (r *concurrentReader) byteRange(pos int64) string {
	return fmt.Sprintf("bytes=%d-%d", pos, min(r.totalBytes-1, pos+r.partSize-1))
}

// releasePermit advances the sliding window by one, letting the dispatcher
// schedule the next part. Each in-flight part holds exactly one permit, so a
// permit is always present when a part is fully delivered.
func (r *concurrentReader) releasePermit() {
	select {
	case <-r.sem:
	default:
	}
}

// fail records the first error and tears the download down.
func (r *concurrentReader) fail(err error) {
	r.err.CompareAndSwap(nil, &err)
	r.teardown()
}

// loadErr returns the recorded error, if any.
func (r *concurrentReader) loadErr() error {
	if p := r.err.Load(); p != nil {
		return *p
	}
	return nil
}

// terminalErr is the error a blocked Read returns once the download has torn
// down: the recorded failure, or io.EOF if teardown was clean.
func (r *concurrentReader) terminalErr() error {
	if err := r.loadErr(); err != nil {
		return err
	}
	return io.EOF
}

// teardown closes done and cancels the worker context exactly once. It is
// invoked on EOF, on error, and by Close.
func (r *concurrentReader) teardown() {
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		close(r.done)
	})
}

type getChunk struct {
	part      int32
	withRange string

	index int32
}

type outChunk struct {
	body  io.Reader
	index int32

	length int64
	cur    int64
}
