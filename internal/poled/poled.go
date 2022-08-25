package poled

import (
	"context"

	"pole/internal/poled/index"
	"pole/internal/poled/meta"
	sqlParser "pole/internal/poled/sql"
	"pole/internal/util/log"

	"github.com/pingcap/tidb/parser/types"
)

type Poled struct {
	conf    *Config
	meta    meta.Meta
	readers *index.Readers
	writers *index.Writers
}

func NewPoled(conf *Config) (*Poled, error) {

	return &Poled{
		meta: meta.Meta{
			MetaData: make(map[string]meta.Mapping),
		},
		readers: index.NewReaders(conf.IndexPath),
		writers: index.NewWriters(),
		conf:    conf,
	}, nil
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
	lg := log.WithField("module", "create_index")
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

	writer, err := index.NewWriter(p.conf.IndexPath)
	if err != nil {
		lg.Error("create index failed:", err)
		return newGeneralResult(err)
	}

	p.writers.Add(idx, writer)

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

func parseFieldType(columnType types.EvalType) meta.FieldType {
	if columnType == types.ETString {
		return meta.FieldTypeText
	}

	if !columnType.IsStringKind() {
		return meta.FieldTypeNumeric
	}

	return meta.FieldTypeUnknown
}
