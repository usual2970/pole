package poled

import (
	"context"
	"time"

	"pole/internal/conf"
	"pole/internal/pb"
	"pole/internal/poled/index"
	"pole/internal/poled/meta"
	sqlParser "pole/internal/poled/sql"
	poleRaft "pole/internal/raft"
	"pole/internal/util/log"

	"github.com/hashicorp/raft"
	"github.com/pingcap/tidb/parser/types"
)

type Poled struct {
	conf    *conf.Config
	meta    *meta.Meta
	readers *index.Readers
	writers *index.Writers
	raft    *raft.Raft
}

func NewPoled(conf *conf.Config, meta *meta.Meta, raft *raft.Raft) (*Poled, error) {

	rs := &Poled{
		meta:    meta,
		readers: index.NewReaders(conf.IndexUri),
		writers: index.NewWriters(conf.IndexUri),
		conf:    conf,
		raft:    raft,
	}
	return rs, nil
}

func (p *Poled) Close() error {
	return nil
}

func (p *Poled) Mapping() map[string]meta.Mapping {
	return p.meta.All()
}

func (p *Poled) Exec(sql string) result {
	stmt, err := sqlParser.Parse(sql)
	if err != nil {
		return newGeneralResult(err)
	}

	switch stmt.ActionType {
	case sqlParser.StmtTypeCreate:
		return p.execCreate(sql, stmt)
	case sqlParser.StmtTypeDrop:
		return p.execDrop(sql, stmt)
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

func (p *Poled) execByRpc(sql string) result {
	client, err := poleRaft.GetClientConn(p.meta.Leader())
	if err != nil {
		return newGeneralResult(err)
	}

	cc := pb.NewPoleClient(client)

	if _, err := cc.Exec(context.Background(), &pb.ExecRequest{Sql: sql}); err != nil {
		return newGeneralResult(err)
	}
	return newGeneralResult(nil)
}

func (p *Poled) execCreate(sql string, stmt *sqlParser.SqlVistor) result {
	if p.raft.State() != raft.Leader {
		return p.execByRpc(sql)
	}
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

	cmd, err := meta.NewAddLogDataCmd(idx, fields)
	if err != nil {
		return newGeneralResult(err)
	}

	p.raft.Apply(cmd, time.Second)

	return newGeneralResult(nil)
}

func (p *Poled) execDrop(sql string, stmt *sqlParser.SqlVistor) result {
	if p.raft.State() != raft.Leader {
		return p.execByRpc(sql)
	}
	idx := stmt.TableName
	if !p.meta.Exists(idx) {
		return newGeneralResult(ErrIndexNotFound)
	}

	cmd, err := meta.NewDeleteLogDataCmd(idx)
	if err != nil {
		return newGeneralResult(err)
	}
	p.raft.Apply(cmd, time.Second)
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
