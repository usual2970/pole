package index

import (
	"sync"

	"github.com/blugelabs/bluge"
)

type Writer struct {
	*bluge.Writer
}

func NewWriter(path string) (*Writer, error) {
	conf := bluge.DefaultConfig(path)
	writer, err := bluge.OpenWriter(conf)
	if err != nil {
		return nil, err
	}
	return &Writer{
		Writer: writer,
	}, nil
}

type Writers struct {
	Writers map[string]*Writer
	sync.RWMutex
}

func NewWriters() *Writers {
	return &Writers{
		Writers: make(map[string]*Writer),
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
	defer w.RUnlock()
	writer, ok := w.Writers[idx]
	return writer, ok
}
