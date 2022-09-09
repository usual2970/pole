package index

import (
	"pole/internal/poled/directory"
	"pole/internal/util/log"
	"sync"

	"github.com/blugelabs/bluge"
	"golang.org/x/sync/singleflight"
)

var wsg singleflight.Group

type Writer struct {
	*bluge.Writer
}

func NewWriter(idx, uri string, lock directory.Lock) (*Writer, error) {
	conf, err := directory.NewIndexConfigWithUri(uri, directory.WithLock(lock), directory.WithIdx(idx))
	if err != nil {
		return nil, err
	}
	writer, err := bluge.OpenWriter(conf)
	if err != nil {
		return nil, err
	}
	return &Writer{
		Writer: writer,
	}, nil
}

type Writers struct {
	Writers  map[string]*Writer
	indexUri string
	sync.RWMutex
	lock directory.Lock
}

func NewWriters(indexUri string, lock directory.Lock) *Writers {
	return &Writers{
		indexUri: indexUri,
		Writers:  make(map[string]*Writer),
		lock:     lock,
	}
}

func (w *Writers) Add(idx string, writer *Writer) {
	w.Lock()
	defer w.Unlock()
	w.Writers[idx] = writer
}

func (w *Writers) Delete(idx string) {
	w.Lock()
	defer w.Unlock()
	delete(w.Writers, idx)
}

func (w *Writers) Get(idx string) (*Writer, bool) {
	w.RLock()
	writer, ok := w.Writers[idx]
	w.RUnlock()
	if ok {
		return writer, true
	}
	lg := log.WithField("module", "get writer")

	rs, err, _ := wsg.Do(idx, func() (interface{}, error) {
		return NewWriter(idx, w.indexUri, w.lock)
	})

	if err != nil {
		lg.Error(err)
		return nil, false
	}

	if rs == nil {
		return nil, false
	}

	writer, ok = rs.(*Writer)
	if !ok {
		return nil, false
	}

	w.Add(idx, writer)

	return writer, true
}

func (w *Writers) All() map[string]*Writer {
	w.RLock()
	defer w.RUnlock()
	return w.Writers
}
