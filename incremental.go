package feedx

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

var defaultCompactFunc = func([]*Reader, *Writer) error {
	return errors.New("TODO! implement")
}

// IncrmentalProduceFunc returns a ProduceFunc closure around an incremental mod time.
type IncrementalProduceFunc func(time.Time) ProduceFunc

// TODO! use ReaderIterator
// CompactionFunc compacts data from multiple readers and outputs to writer
type CompactionFunc func([]*Reader, *Writer) error

// IncrementalProducer produces a continuous incremental feed.
type IncrementalProducer struct {
	producerState

	bucket    bfs.Bucket
	manifest  *bfs.Object
	ownBucket bool
	opt       IncrementalProducerOptions

	ctx  context.Context
	stop context.CancelFunc
	ipfn IncrementalProduceFunc

	lastCompact int64
}

// IncrementalProducerOptions configure the producer instance.
type IncrementalProducerOptions struct {
	ProducerOptions
	CompactionOptions
}

type CompactionOptions struct {
	copt ConsumerOptions

	CompactFunc CompactionFunc
}

func (o *IncrementalProducerOptions) norm(lmfn LastModFunc) {
	if o.Compression == nil {
		o.Compression = GZipCompression
	}
	if o.Format == nil {
		o.Format = ProtobufFormat
	}
	if o.Interval == 0 {
		o.Interval = time.Minute
	}
	o.LastModCheck = lmfn

	// set defaults for compaction
	o.copt.Compression = o.Compression
	o.copt.Format = o.Format
	if o.copt.Interval == 0 {
		o.copt.Interval = time.Hour
	}
	if o.CompactFunc == nil {
		o.CompactionOptions.CompactFunc = defaultCompactFunc
	}
}

// NewIncrementalProducer inits a new incremental feed producer.
func NewIncrementalProducer(ctx context.Context, bucketURL string, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	p, err := NewIncrementalProducerForBucket(ctx, bucket, opt, lmfn, ipfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	p.ownBucket = true

	return p, nil
}

// NewIncrementalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	var o IncrementalProducerOptions
	if opt != nil {
		o = IncrementalProducerOptions(*opt)
	}
	o.norm(lmfn)

	ctx, stop := context.WithCancel(ctx)
	p := &IncrementalProducer{
		bucket:   bucket,
		manifest: bfs.NewObjectFromBucket(bucket, "manifest.json"),
		ctx:      ctx,
		stop:     stop,
		opt:      o,
		ipfn:     ipfn,
	}

	// run initial push
	if _, err := p.push(); err != nil {
		_ = p.Close()
		return nil, err
	}

	// start continuous loop
	// use error group
	go p.loop()

	return p, nil
}

// Close stops the producer.
func (p *IncrementalProducer) Close() (err error) {
	p.stop()
	if e := p.manifest.Close(); e != nil {
		err = e
	}

	if p.ownBucket {
		if e := p.bucket.Close(); e != nil {
			err = e
		}
	}
	return
}

// LastCompact returns time of last compact attempt.
func (p *IncrementalProducer) LastCompact() time.Time {
	return timestamp(atomic.LoadInt64(&p.lastCompact)).Time()
}

func (p *IncrementalProducer) loop() {
	tickerA := time.NewTicker(p.opt.Interval)
	defer tickerA.Stop()

	tickerB := time.NewTicker(p.opt.copt.Interval)
	defer tickerB.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-tickerA.C:
			state, err := p.push()
			if p.opt.AfterPush != nil {
				p.opt.AfterPush(state, err)
			}
		case <-tickerB.C:
			// TODO what should it return?  maybe just use existing AfterPush opt?
			_ = p.compact()
		}
	}
}

func (p *IncrementalProducer) push() (*ProducerPush, error) {
	start := time.Now()
	p.producerState.updateLastPush(start)

	// get last mod time for local records
	localLastMod, err := p.opt.LastModCheck(p.ctx)
	if err != nil {
		return nil, err
	}
	if localLastMod.IsZero() {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	// fetch manifest from remote
	manifest, err := loadManifest(p.ctx, p.manifest)
	if err != nil {
		return nil, err
	}

	// compare manifest LastModified to local last mod.
	remoteLastMod := manifest.LastModified
	if remoteLastMod == timestampFromTime(localLastMod) {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	wopt := p.opt.WriterOptions
	wopt.LastMod = localLastMod

	// write data modified since last remote mod
	numWritten, err := p.writeDataFile(manifest, &wopt)
	if err != nil {
		return nil, err
	}
	// write new manifest to remote
	if err := p.commitManifest(manifest, &WriterOptions{LastMod: wopt.LastMod}); err != nil {
		return nil, err
	}

	p.producerState.updateNumWritten(numWritten)
	p.producerState.updateLastModified(wopt.LastMod)
	return &ProducerPush{producerState: p.producerState, Updated: true}, nil
}

func (p *IncrementalProducer) writeDataFile(m *manifest, wopt *WriterOptions) (int, error) {
	fname := m.newDataFileName(wopt)

	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, fname), wopt)
	defer writer.Discard()

	if err := p.ipfn(wopt.LastMod)(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}

	m.Files = append(m.Files, fname)
	m.LastModified = timestampFromTime(wopt.LastMod)

	return writer.NumWritten(), nil
}

func (p *IncrementalProducer) commitManifest(m *manifest, wopt *WriterOptions) error {
	name := p.manifest.Name()
	wopt.norm(name) // norm sets writer format and compression from name

	writer := NewWriter(p.ctx, p.manifest, wopt)
	defer writer.Discard()

	if err := writer.Encode(m); err != nil {
		return err
	}
	return writer.Commit()
}

type CompactOptions struct {
}

func (p *IncrementalProducer) compact() error {
	compactTime := timestampFromTime(time.Now()).Millis()
	defer func() {
		atomic.StoreInt64(&p.lastCompact, compactTime)
	}()

	// fetch manifest from remote
	manifest, err := loadManifest(p.ctx, p.manifest)
	if err != nil {
		return err
	}

	// TODO! which files are we compacting?  all of them?  just the oldest N files? For now assume all
	files := manifest.Files

	// build reader slice & get max modified
	// TODO! can use ReaderIterator once approved so dont have to defer r.Close()
	var maxMod timestamp
	readers := make([]*Reader, 0, len(manifest.Files))
	for _, file := range files {
		obj := bfs.NewObjectFromBucket(p.bucket, file) // dont need to defer Close as inited from bucket
		r, err := NewReader(p.ctx, obj, &p.opt.copt.ReaderOptions)
		if err != nil {
			return err
		}
		defer r.Close()

		readers = append(readers, r)

		ts, err := remoteLastModified(p.ctx, obj)
		if err != nil {
			return err
		}
		if ts > maxMod {
			maxMod = ts
		}
	}

	// init writer for compacted file
	manifest.Generation++
	wopt := p.opt.WriterOptions
	wopt.LastMod = maxMod.Time()
	fname := manifest.newDataFileName(&wopt)
	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, fname), &wopt)
	defer writer.Discard()

	// run file compaction
	if err := p.opt.CompactFunc(readers, writer); err != nil {
		return err
	}

	if err := writer.Commit(); err != nil {
		return err
	}

	// rewrite the remote manifest
	manifest.Files = append(manifest.Files, fname)
	manifest.LastModified = timestampFromTime(wopt.LastMod)

	return p.commitManifest(manifest, &WriterOptions{LastMod: wopt.LastMod})
}
