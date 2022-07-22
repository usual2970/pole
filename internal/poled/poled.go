package poled

import (
	"context"
	"fmt"

	"github.com/blugelabs/bluge"
	"github.com/pingcap/tidb/parser/types"
	"pole/internal/poled/index"
	"pole/internal/util/log"
)

type poled struct {
	meta    meta
	readers *index.Readers
	writers *index.Writers
}

func NewPoled() (*poled, error) {

	return &poled{
		meta: meta{
			MetaData: make(map[string]map[string]filedOptions),
		},
		readers: index.NewReaders(),
		writers: index.NewWriters(),
	}, nil
}

func (p *poled) Exec(sql string) result {
	stmt, err := parse(sql)
	if err != nil {
		return newGeneralResult(err)
	}

	switch stmt.actionType {
	case stmtTypeCreate:
		return p.execCreate(stmt)
	case stmtTypeDrop:
		return p.execDrop(stmt)
	case stmtTypeInsert:
		return p.execInsert(stmt)
	case stmtTypeDelete:
		return p.execDelete(stmt)
	case stmtTypeUpdate:
		return p.execUpdate(stmt)
	case stmtTypeSelect:
		return p.execSelect(stmt)
	}
	return newGeneralResult(ErrSyntaxNotSupported)
}
func (p *poled) execSelect(stmt *sqlRs) *generalResult {
	idx := stmt.tableName
	lg := log.WithField("module", "delete_index").WithField("index", idx)
	_, exists := p.meta.Get(idx)
	if !exists {
		lg.Error(ErrIndexNotFound)
		return newGeneralResult(ErrIndexNotFound)
	}

	reader, exists := p.readers.Get(idx)
	var newReaderErr error
	if !exists {
		reader, newReaderErr = index.NewReader()
		if newReaderErr != nil {
			return newGeneralResult(newReaderErr)
		}

		p.readers.Add(idx, reader)
	}

	query := bluge.NewMatchAllQuery()
	req := bluge.NewTopNSearch(100, query).WithStandardAggregations().
		IncludeLocations().
		ExplainScores()
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

func (p *poled) execDelete(stmt *sqlRs) *generalResult {
	idx := stmt.tableName
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

	batch, err := stmt.buildDeleteBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}

	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	return newGeneralResult(nil)
}

func (p *poled) execUpdate(stmt *sqlRs) *generalResult {
	idx := stmt.tableName
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

	batch, err := stmt.buildUpdateBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}

	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	return newGeneralResult(nil)

}

func (p *poled) execInsert(stmt *sqlRs) *generalResult {
	idx := stmt.tableName
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

	batch, err := stmt.buildInsertBatch(meta)
	if err != nil {
		lg.Error(err)
		return newGeneralResult(err)
	}
	if err := writer.Batch(batch); err != nil {
		lg.Error(ErrBatchFailed)
		return newGeneralResult(err)
	}

	return newGeneralResult(nil)
}

func (p *poled) execCreate(stmt *sqlRs) *generalResult {
	lg := log.WithField("module", "create_index")
	idx := stmt.tableName
	if p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexExist)
	}
	fields := make(map[string]filedOptions)
	for _, column := range stmt.colNames {
		fields[column.name] = filedOptions{
			Type:   parseFieldType(column.typ),
			Option: option{},
		}
	}

	writer, err := index.NewWriter()
	if err != nil {
		lg.Error("create index failed:", err)
		return newGeneralResult(err)
	}

	p.writers.Add(idx, writer)

	p.meta.Add(idx, fields)
	return newGeneralResult(nil)
}

func (p *poled) execDrop(stmt *sqlRs) *generalResult {
	idx := stmt.tableName
	if !p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexNotFound)
	}
	p.meta.Delete(idx)
	p.readers.Delete(idx)
	p.writers.Delete(idx)
	return newGeneralResult(nil)
}

func parseFieldType(columnType types.EvalType) fieldType {
	if columnType == types.ETString {
		return fieldTypeText
	}

	if !columnType.IsStringKind() {
		return fieldTypeNumeric
	}

	return fieldTypeUnknown
}
