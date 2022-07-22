package index

import (
	"sync"

	"github.com/blugelabs/bluge"
)

type Reader struct {
	*bluge.Reader
}

func NewReader() (*Reader, error) {
	conf := bluge.DefaultConfig("/var/www/go/pole/tests/indexes")
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return nil, err
	}
	return &Reader{
		Reader: reader,
	}, nil
}

type Readers struct {
	Readers map[string]*Reader
	sync.RWMutex
}

func NewReaders() *Readers {
	return &Readers{
		Readers: make(map[string]*Reader),
	}
}

func (r *Readers) Get(idx string) (*Reader, bool) {
	r.RLock()
	defer r.RUnlock()
	reader, ok := r.Readers[idx]
	return reader, ok
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
