package transfermanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3testing "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/internal/testing"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestConcurrentReader(t *testing.T) {
	cases := map[string]struct {
		partSize     int64
		partsCount   int32
		sectionParts int32
		getObjectFn  func(*s3testing.TransferManagerLoggingClient, *s3.GetObjectInput) (*s3.GetObjectOutput, error)
		options      Options
	}{
		"part get single goroutine": {
			partSize:     10,
			partsCount:   1000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   1,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get single goroutine with only one section": {
			partSize:     1000,
			partsCount:   5,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   3,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get single goroutine with only one part": {
			partSize:     1000,
			partsCount:   1,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   3,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get multiple goroutines": {
			partSize:     10,
			partsCount:   1000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   5,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get multiple goroutines with only one section": {
			partSize:     10,
			partsCount:   6,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   5,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get multiple goroutines with only one part": {
			partSize:     10,
			partsCount:   1,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   5,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"part get multiple goroutines with large part size": {
			partSize:     10000,
			partsCount:   10000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectParts,
				Concurrency:   5,
			},
			getObjectFn: s3testing.ReaderPartGetObjectFn,
		},
		"range get single goroutine": {
			partSize:     10,
			partsCount:   1000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   1,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get single goroutine with only one section": {
			partSize:     1000,
			partsCount:   5,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   3,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get single goroutine with only one part": {
			partSize:     1000,
			partsCount:   1,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   3,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get multiple goroutines": {
			partSize:     10,
			partsCount:   1000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   5,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get multiple goroutines with only one section": {
			partSize:     10,
			partsCount:   6,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   5,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get multiple goroutines with only one part": {
			partSize:     10,
			partsCount:   1,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   5,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
		"range get multiple goroutines with large part size": {
			partSize:     10000,
			partsCount:   10000,
			sectionParts: 6,
			options: Options{
				GetObjectType: types.GetObjectRanges,
				Concurrency:   5,
			},
			getObjectFn: s3testing.RangeGetObjectFn,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s3Client := &s3testing.TransferManagerLoggingClient{}
			s3Client.GetObjectFn = c.getObjectFn

			r := &concurrentReader{
				partSize:   c.partSize,
				partsCount: c.partsCount,
				options:    c.options,
				in: &GetObjectInput{
					Bucket: aws.String("bucket"),
					Key:    aws.String("key"),
				},
			}
			// Derive the sliding-window width the same way get() does, using a
			// buffer sized to sectionParts so multi-goroutine cases exercise
			// real parallelism.
			r.window = min(getWindow(c.options.Concurrency, int64(c.sectionParts)*c.partSize, c.partSize), c.partsCount)

			expectBuf := make([]byte, 0)
			expectPartsData := make([][]byte, c.partsCount)
			for i := int32(0); i < c.partsCount; i++ {
				b := make([]byte, c.partSize)
				if i == c.partsCount-1 {
					b = make([]byte, rand.Intn(int(c.partSize))+1)
				}
				rand.Read(b)
				expectBuf = append(expectBuf, b...)
				expectPartsData[i] = b
			}
			s3Client.Data = expectBuf
			s3Client.PartsData = expectPartsData
			r.options.S3 = s3Client
			r.totalBytes = int64(len(expectBuf))
			r.initReader(ctx)

			actualBuf, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("expect no error when reading, got %v", err)
			}

			if e, a := len(expectBuf), len(actualBuf); e != a {
				t.Errorf("expect data sent to have length %d, but got %d", e, a)
			}
			if e, a := expectBuf, actualBuf; !bytes.Equal(e, a) {
				t.Errorf("expect data sent to be %v, got %v", e, a)
			}
		})
	}
}

// TestConcurrentReaderReadRepeatAfterError verifies that once a download hits a
// terminal error the error is sticky: repeated reads return the same error and
// do not schedule any further GetObject calls. With a persistent dispatcher the
// exact call count is no longer fixed, so this asserts behavior rather than an
// exact number.
func TestConcurrentReaderReadRepeatAfterError(t *testing.T) {
	ctx := context.Background()
	s3Client := &s3testing.TransferManagerLoggingClient{}
	s3Client.GetObjectFn = s3testing.ErrRangeGetObjectFn
	s3Client.Data = []byte("abcdefghijkl")

	r := &concurrentReader{
		partSize:   4,
		partsCount: 3,
		options: Options{
			GetObjectType: types.GetObjectRanges,
			Concurrency:   1,
			S3:            s3Client,
		},
		in: &GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		},
		totalBytes: int64(len(s3Client.Data)),
	}
	r.window = 1
	r.initReader(ctx)

	buf := make([]byte, 4)
	var readErr error
	for i := 0; i < 100 && readErr == nil; i++ {
		_, readErr = r.Read(buf)
	}
	if readErr == nil {
		t.Fatal("expected read to eventually return an error")
	}
	if readErr == io.EOF {
		t.Fatal("expected a service error, got io.EOF")
	}

	stored := r.loadErr()
	if stored == nil {
		t.Fatal("expected the reader to record the error")
	}
	if !errors.Is(readErr, stored) {
		t.Fatalf("expected read to return stored error, got %v and stored %v", readErr, stored)
	}

	invocationsAfterError := s3Client.GetObjectInvocations

	_, err := r.Read(buf)
	if !errors.Is(err, stored) {
		t.Fatalf("expected repeated read to return stored error, got %v and stored %v", err, stored)
	}
	if got := s3Client.GetObjectInvocations; got != invocationsAfterError {
		t.Fatalf("expected no new GetObject calls after error, got %d vs %d", got, invocationsAfterError)
	}
}

// TestConcurrentReaderInFlightWindow verifies the reader keeps up to window
// parts in flight — bounded by Concurrency and by GetObjectBufferSize/partSize
// — and never exceeds it. The buffer-limited case doubles as the memory-cap
// test: in-flight stays at k even when Concurrency is far larger.
func TestConcurrentReaderInFlightWindow(t *testing.T) {
	cases := map[string]struct {
		concurrency int
		bufferParts int64
		partSize    int64
		parts       int64
		wantWindow  int32
	}{
		"limited by concurrency":         {concurrency: 8, bufferParts: 32, partSize: 16, parts: 64, wantWindow: 8},
		"limited by buffer":              {concurrency: 32, bufferParts: 8, partSize: 16, parts: 64, wantWindow: 8},
		"wide window beats old cap of 6": {concurrency: 16, bufferParts: 16, partSize: 16, parts: 64, wantWindow: 16},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			data := make([]byte, c.partSize*c.parts)
			for i := range data {
				data[i] = byte(i % 251)
			}
			s3Client := &s3testing.TransferManagerLoggingClient{Data: data}

			release := make(chan struct{})
			s3Client.GetObjectFn = blockingRangeGetObjectFn(release)

			mgr := New(s3Client, func(o *Options) {
				o.GetObjectType = types.GetObjectRanges
				o.Concurrency = c.concurrency
				o.PartSizeBytes = c.partSize
				o.GetObjectBufferSize = c.bufferParts * c.partSize
			})

			// Release the gate once the window is saturated (or bail out on a
			// timeout so a regression fails instead of hanging).
			go func() {
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					if s3Client.MaxInFlight.Load() >= c.wantWindow {
						break
					}
					time.Sleep(time.Millisecond)
				}
				close(release)
			}()

			out, err := mgr.GetObject(context.Background(), &GetObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String("key"),
			})
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			got, err := io.ReadAll(out.Body)
			if err != nil {
				t.Fatalf("expected no read error, got %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("downloaded data mismatch: got %d bytes, want %d", len(got), len(data))
			}
			if a := s3Client.MaxInFlight.Load(); a != c.wantWindow {
				t.Errorf("expected max in-flight %d, got %d", c.wantWindow, a)
			}
		})
	}
}

// TestConcurrentReaderRetriesBodyError verifies transient body-read failures
// are retried per part up to PartBodyMaxRetries, and the download then succeeds.
func TestConcurrentReaderRetriesBodyError(t *testing.T) {
	data := []byte("hello concurrent world")
	var calls int32
	const failUntil = 2

	s3Client := &s3testing.TransferManagerLoggingClient{Data: data}
	s3Client.GetObjectFn = func(cl *s3testing.TransferManagerLoggingClient, params *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		n := atomic.AddInt32(&calls, 1)
		body := cl.Data
		if n <= failUntil {
			// Deliver one byte short of the advertised length so io.ReadFull
			// reports ErrUnexpectedEOF, which the reader wraps and retries.
			body = cl.Data[:len(cl.Data)-1]
		}
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: aws.Int64(int64(len(cl.Data))),
			ETag:          aws.String("myetag"),
		}, nil
	}

	mgr := New(s3Client, func(o *Options) {
		o.GetObjectType = types.GetObjectRanges
		o.Concurrency = 1
		o.PartBodyMaxRetries = 5
	})

	out, err := mgr.GetObject(context.Background(), &GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("expected no read error, got %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("expected %q, got %q", data, got)
	}
	if a := atomic.LoadInt32(&calls); a != failUntil+1 {
		t.Errorf("expected %d GetObject attempts, got %d", failUntil+1, a)
	}
}

// TestConcurrentReaderClose verifies Close aborts an in-progress download,
// is idempotent, and lets the worker goroutines quiesce.
func TestConcurrentReaderClose(t *testing.T) {
	partSize := int64(16)
	parts := int64(64)
	data := make([]byte, partSize*parts)
	for i := range data {
		data[i] = byte(i % 251)
	}

	s3Client := &s3testing.TransferManagerLoggingClient{Data: data}
	release := make(chan struct{})
	s3Client.GetObjectFn = blockingRangeGetObjectFn(release)

	const concurrency = 8
	mgr := New(s3Client, func(o *Options) {
		o.GetObjectType = types.GetObjectRanges
		o.Concurrency = concurrency
		o.PartSizeBytes = partSize
		o.GetObjectBufferSize = concurrency * partSize
	})

	out, err := mgr.GetObject(context.Background(), &GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	closer, ok := out.Body.(io.Closer)
	if !ok {
		t.Fatal("expected GetObject body to implement io.Closer")
	}

	readErrCh := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(out.Body)
		readErrCh <- err
	}()

	// Wait until the window is saturated with blocked in-flight requests.
	waitFor(t, 2*time.Second, func() bool {
		return s3Client.InFlight.Load() >= concurrency
	})

	if err := closer.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	// Unblock the in-flight requests; workers should discard and exit.
	close(release)

	waitFor(t, 2*time.Second, func() bool {
		return s3Client.InFlight.Load() == 0
	})

	select {
	case <-readErrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not return after Close")
	}
}

// TestConcurrentReaderContextCancel verifies that canceling the caller's
// context aborts an in-progress download promptly (no hang) and lets the
// worker goroutines quiesce, even when the caller never calls Close.
func TestConcurrentReaderContextCancel(t *testing.T) {
	partSize := int64(16)
	parts := int64(64)
	data := make([]byte, partSize*parts)
	for i := range data {
		data[i] = byte(i % 251)
	}

	s3Client := &s3testing.TransferManagerLoggingClient{Data: data}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s3Client.GetObjectFn = func(cl *s3testing.TransferManagerLoggingClient, params *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		// Simulate a request that is in flight when the context is canceled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return nil, fmt.Errorf("request not canceled in time")
		}
	}

	const concurrency = 8
	mgr := New(s3Client, func(o *Options) {
		o.GetObjectType = types.GetObjectRanges
		o.Concurrency = concurrency
		o.PartSizeBytes = partSize
		o.GetObjectBufferSize = concurrency * partSize
	})

	out, err := mgr.GetObject(ctx, &GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	readErrCh := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(out.Body)
		readErrCh <- err
	}()

	waitFor(t, 2*time.Second, func() bool {
		return s3Client.InFlight.Load() >= concurrency
	})

	cancel()

	select {
	case err := <-readErrCh:
		if err == nil {
			t.Fatal("expected a non-nil error after context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not return after context cancellation")
	}

	waitFor(t, 2*time.Second, func() bool {
		return s3Client.InFlight.Load() == 0
	})
}

// blockingRangeGetObjectFn returns a range GetObject stub that blocks until
// release is closed, then serves the requested byte range from Data.
func blockingRangeGetObjectFn(release <-chan struct{}) func(*s3testing.TransferManagerLoggingClient, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return func(cl *s3testing.TransferManagerLoggingClient, params *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		<-release
		start, fin, err := getReqRange(aws.ToString(params.Range))
		if err != nil {
			return nil, err
		}
		fin++
		if fin > int64(len(cl.Data)) {
			fin = int64(len(cl.Data))
		}
		body := cl.Data[start:fin]
		out := &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: aws.Int64(int64(len(body))),
			ETag:          aws.String("myetag"),
		}
		if len(body) != len(cl.Data) {
			out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(cl.Data)))
		}
		return out, nil
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
