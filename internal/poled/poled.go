package poled

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"pole/internal/poled/index"
	"pole/internal/poled/meta"
	sqlParser "pole/internal/poled/sql"
	"pole/internal/util/log"

	"github.com/pingcap/tidb/parser/types"
)

type Poled struct {
	conf    *Config
	meta    *meta.Meta
	readers *index.Readers
	writers *index.Writers
}

func NewPoled(conf *Config) (*Poled, error) {

	rs := &Poled{
		meta: &meta.Meta{
			MetaData: make(map[string]meta.Mapping),
		},
		readers: index.NewReaders(conf.IndexUri),
		writers: index.NewWriters(conf.IndexUri),
		conf:    conf,
	}
	if err := rs.loadMetaData(); err != nil {
		return nil, err
	}
	return rs, nil
}

func (p *Poled) Close() error {
	return p.persistentMetaData()
}

func (p *Poled) Exec(sql string) result {
	stmt, err := sqlParser.Parse(sql)
	if err != nil {
		return newGeneralResult(err)
	}

	switch stmt.ActionType {
	case sqlParser.StmtTypeCreate:
		return p.execCreate(stmt)
	case sqlParser.StmtTypeDrop:
		return p.execDrop(stmt)
	case sqlParser.StmtTypeInsert:
		return p.execInsert(stmt)
	case sqlParser.StmtTypeDelete:
		return p.execDelete(stmt)
	case sqlParser.StmtTypeUpdate:
		return p.execUpdate(stmt)
	case sqlParser.StmtTypeSelect:
		return p.execSelect(stmt)
	}
	return newGeneralResult(ErrSyntaxNotSupported)
}
func (p *Poled) execSelect(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	lg := log.WithField("module", "exec select").WithField("index", idx)
	meta, exists := p.meta.Get(idx)
	if !exists {
		lg.Error(ErrIndexNotFound)
		return newGeneralResult(ErrIndexNotFound)
	}

	reader, exists := p.readers.Get(idx)
	if !exists {
		return newGeneralResult(ErrReaderNotFound)
	}
	req, err := stmt.BuildRequest(meta)
	if err != nil {
		return newGeneralResult(err)
	}
	iter, err := reader.Search(context.Background(), req)
	if err != nil {
		return newGeneralResult(err)
	}
	return newSelectResult(iter, meta, stmt.ColNames, stmt.SelectAll)
}

func (p *Poled) execDelete(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	lg := log.WithField("module", "delete_index").WithField("index", idx)
	meta, exists := p.meta.Get(idx)
	if !exists {
		lg.Error(ErrIndexNotFound)
		return newGeneralResult(ErrIndexNotFound)
	}

	writer, exists := p.writers.Get(idx)
	if !exists {
		lg.Error(ErrWriterNotFound)
		return newGeneralResult(ErrWriterNotFound)
	}

	batch, err := stmt.BuildDeleteBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}

	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	p.readers.Delete(idx)

	return newGeneralResult(nil)
}

func (p *Poled) execUpdate(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	lg := log.WithField("module", "update_index").WithField("index", idx)
	meta, exists := p.meta.Get(idx)
	if !exists {
		lg.Error(ErrIndexNotFound)
		return newGeneralResult(ErrIndexNotFound)
	}

	writer, exists := p.writers.Get(idx)
	if !exists {
		lg.Error(ErrWriterNotFound)
		return newGeneralResult(ErrWriterNotFound)
	}

	batch, err := stmt.BuildUpdateBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}

	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	p.readers.Delete(idx)

	return newGeneralResult(nil)

}

func (p *Poled) execInsert(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	lg := log.WithField("module", "insert_index").WithField("index", idx)
	meta, exists := p.meta.Get(idx)
	if !exists {
		lg.Error(ErrIndexNotFound)
		return newGeneralResult(ErrIndexNotFound)
	}

	writer, exists := p.writers.Get(idx)
	if !exists {
		lg.Error(ErrWriterNotFound)
		return newGeneralResult(ErrWriterNotFound)
	}

	batch, err := stmt.BuildInsertBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}
	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	p.readers.Delete(idx)

	return newGeneralResult(nil)
}

func (p *Poled) execCreate(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	if p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexExist)
	}
	fields := meta.Mapping{Properties: map[string]meta.FiledOptions{}}
	for _, column := range stmt.ColNames {
		fields.Properties[column.Name] = meta.FiledOptions{
			Type:   parseFieldType(column.Typ),
			Option: meta.Option{},
		}
	}

	if _, ok := p.writers.Get(p.conf.IndexUri); !ok {
		return newGeneralResult(ErrWriterCreateFailed)
	}

	p.meta.Add(idx, fields)

	return newGeneralResult(nil)
}

func (p *Poled) execDrop(stmt *sqlParser.SqlVistor) result {
	idx := stmt.TableName
	if !p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexNotFound)
	}
	p.meta.Delete(idx)
	p.readers.Delete(idx)
	p.writers.Delete(idx)
	return newGeneralResult(nil)
}

func (p *Poled) loadMetaData() error {
	fn := newMetaDataFile(GetDataPath())
	raw, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if raw == nil {
		return nil
	}
	data := &meta.Meta{}
	if err := json.Unmarshal(raw, data); err != nil {
		return err
	}
	p.meta = data
	return nil
}

func (p *Poled) persistentMetaData() error {
	raw, err := json.Marshal(p.meta)
	if err != nil {
		return err
	}
	fn := newMetaDataFile(GetDataPath())

	return writeSyncFile(fn, raw)
}

func parseFieldType(columnType types.EvalType) meta.FieldType {
	if columnType == types.ETString {
		return meta.FieldTypeText
	}

	if !columnType.IsStringKind() {
		return meta.FieldTypeNumeric
	}

	return meta.FieldTypeUnknown
}

func newMetaDataFile(filePath string) string {
	return filepath.Join(filePath, "pole.dat")
}

func readOrEmpty(filePath string) ([]byte, error) {
	rs, err := ioutil.ReadFile(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", filePath, err)
		}
	}
	return rs, nil
}

func writeSyncFile(filePath string, data []byte) error {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}
