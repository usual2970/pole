package poled

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/parser/types"
	"pole/internal/poled/index"
	"pole/internal/poled/meta"
	sqlParser "pole/internal/poled/sql"
	"pole/internal/util/log"
)

type poled struct {
	conf    *Config
	meta    meta.Meta
	readers *index.Readers
	writers *index.Writers
}

func NewPoled(conf *Config) (*poled, error) {

	return &poled{
		meta: meta.Meta{
			MetaData: make(map[string]map[string]meta.FiledOptions),
		},
		readers: index.NewReaders(conf.IndexPath),
		writers: index.NewWriters(),
		conf:    conf,
	}, nil
}

func (p *poled) Exec(sql string) result {
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
func (p *poled) execSelect(stmt *sqlParser.SqlVistor) *generalResult {
	idx := stmt.TableName
	lg := log.WithField("module", "delete_index").WithField("index", idx)
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

	next, err := iter.Next()
	for err == nil && next != nil {
		err = next.VisitStoredFields(func(field string, value []byte) bool {

			fmt.Println(field, string(value))
			return true
		})
		if err != nil {
			return newGeneralResult(err)
		}
		next, err = iter.Next()
	}
	if err != nil {
		return newGeneralResult(err)
	}

	return newGeneralResult(nil)
}

func (p *poled) execDelete(stmt *sqlParser.SqlVistor) *generalResult {
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

func (p *poled) execUpdate(stmt *sqlParser.SqlVistor) *generalResult {
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

func (p *poled) execInsert(stmt *sqlParser.SqlVistor) *generalResult {
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

func (p *poled) execCreate(stmt *sqlParser.SqlVistor) *generalResult {
	lg := log.WithField("module", "create_index")
	idx := stmt.TableName
	if p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexExist)
	}
	fields := make(map[string]meta.FiledOptions)
	for _, column := range stmt.ColNames {
		fields[column.Name] = meta.FiledOptions{
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

func (p *poled) execDrop(stmt *sqlParser.SqlVistor) *generalResult {
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
