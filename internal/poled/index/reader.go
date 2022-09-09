package index

import (
	"pole/internal/poled/directory"
	"pole/internal/util/log"
	"sync"

	"github.com/blugelabs/bluge"
	"golang.org/x/sync/singleflight"
)

var sg singleflight.Group

type Reader struct {
	*bluge.Reader
}

func NewReader(idx, uri string, lock directory.Lock) (*Reader, error) {
	conf, err := directory.NewIndexConfigWithUri(uri, directory.WithLock(lock), directory.WithIdx(idx))
	if err != nil {
		return nil, err
	}
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return nil, err
	}
	return &Reader{
		Reader: reader,
	}, nil
}

type Readers struct {
	Readers  map[string]*Reader
	indexUri string
	sync.RWMutex
	lock directory.Lock
}

func NewReaders(indexUri string, lock directory.Lock) *Readers {
	return &Readers{
		Readers:  make(map[string]*Reader),
		indexUri: indexUri,
		lock:     lock,
	}
}

func (r *Readers) Get(idx string) (*Reader, bool) {
	r.RLock()
	reader, ok := r.Readers[idx]
	r.RUnlock()
	if ok {
		return reader, ok
	}

	lg := log.WithField("module", "get reader")

	rs, err, _ := sg.Do(idx, func() (interface{}, error) {
		return NewReader(idx, r.indexUri, r.lock)
	})

	if err != nil {
		lg.Error(err)
		return nil, false
	}

	if rs == nil {
		return nil, false
	}

	reader, ok = rs.(*Reader)
	if !ok {
		return nil, false
	}

	r.Add(idx, reader)

	return reader, true
}

func (r *Readers) Add(idx string, reader *Reader) {
	r.Lock()
	defer r.Unlock()
	r.Readers[idx] = reader
}

func (r *Readers) Delete(idx string) {
	r.Lock()
	defer r.Unlock()
	delete(r.Readers, idx)
}

func (r *Readers) Clear(idx string) {
	reader, ok := r.Get(idx)
	if !ok {
		return
	}

	r.Delete(idx)

	_ = reader.Close()

}
