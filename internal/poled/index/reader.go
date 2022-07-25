package index

import (
	"sync"

	"github.com/blugelabs/bluge"
	"golang.org/x/sync/singleflight"
)

var sg singleflight.Group

type Reader struct {
	*bluge.Reader
}

func NewReader(path string) (*Reader, error) {
	conf := bluge.DefaultConfig(path)
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return nil, err
	}
	return &Reader{
		Reader: reader,
	}, nil
}

type Readers struct {
	Readers   map[string]*Reader
	indexPath string
	sync.RWMutex
}

func NewReaders(indexPath string) *Readers {
	return &Readers{
		Readers:   make(map[string]*Reader),
		indexPath: indexPath,
	}
}

func (r *Readers) Get(idx string) (*Reader, bool) {
	r.RLock()
	reader, ok := r.Readers[idx]
	r.RUnlock()
	if ok {
		return reader, ok
	}

	rs, err, _ := sg.Do(idx, func() (interface{}, error) {
		return NewReader(r.indexPath)
	})

	if err != nil {
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
