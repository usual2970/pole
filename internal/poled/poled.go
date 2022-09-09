package poled

import (
	"context"
	"fmt"
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
		meta: meta,
		conf: conf,
		raft: raft,
	}

	rs.readers = index.NewReaders(conf.IndexUri, rs)
	rs.writers = index.NewWriters(conf.IndexUri, rs)
	return rs, nil
}

func (p *Poled) Close() error {
	lg := log.WithField("module", "poleClose")
	if p.isLearder() {
		for _, writer := range p.writers.All() {
			err := writer.Close()
			lg.Error(err)
		}
	}
	lg.Info("closed")
	return nil
}

func (p *Poled) Mapping() map[string]meta.Mapping {
	return p.meta.All()
}

func (p *Poled) isLearder() bool {
	return p.raft.State() == raft.Leader
}

func (p *Poled) Exec(sql string) result {
	stmt, err := sqlParser.Parse(sql)
	if err != nil {
		return newGeneralResult(err)
	}

	if stmt.ActionType == sqlParser.StmtTypeSelect {
		return p.execSelect(stmt)
	}

	if p.raft.State() != raft.Leader {
		rs := p.execByRpc(sql)
		p.readers.Delete(stmt.TableName)
		return rs
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
	lg.Info("insert success:", batch)
	return newGeneralResult(nil)
}

func (p *Poled) execByRpc(sql string) result {
	lg := log.WithField("module", "execByRpc").WithField("state", p.raft.State().String()).WithField("leaderGrpcAddr", p.meta.Leader())

	client, err := poleRaft.GetClientConn(p.meta.Leader())
	if err != nil {
		return newGeneralResult(err)
	}

	cc := pb.NewPoleClient(client)

	if _, err := cc.Exec(context.Background(), &pb.ExecRequest{Sql: sql}); err != nil {
		lg.Error("failed to execute ,err: ", err)
		return newGeneralResult(err)
	}
	lg.Info("exec success")
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

	if _, ok := p.writers.Get(idx); !ok {
		return newGeneralResult(ErrWriterCreateFailed)
	}

	cmd, err := meta.NewAddLogDataCmd(idx, fields)
	if err != nil {
		return newGeneralResult(err)
	}

	p.raft.Apply(cmd, time.Second)

	return newGeneralResult(nil)
}

func (p *Poled) execDrop(stmt *sqlParser.SqlVistor) result {
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

func (p *Poled) Lock(lockUri string) error {
	if p.raft.State() != raft.Leader {
		return p.lockByGrpc(lockUri)
	}
	cmd, _ := meta.NewLockCmd(lockUri)
	af := p.raft.Apply(cmd, time.Millisecond*200)

	if af.Error() != nil {
		return af.Error()
	}

	rs := af.Response()
	if err, ok := rs.(error); ok {
		return err
	}

	return nil
}

func (p *Poled) Unlock(lockUri string) error {
	if p.raft.State() != raft.Leader {
		return p.unlockByGrpc(lockUri)
	}
	cmd, _ := meta.NewUnLockCmd(lockUri)
	af := p.raft.Apply(cmd, time.Millisecond*200)

	if af.Error() != nil {
		return af.Error()
	}

	rs := af.Response()
	if err, ok := rs.(error); ok {
		return err
	}

	return nil
}

func (p *Poled) lockByGrpc(lockUri string) error {
	client, err := poleRaft.GetClientConn(p.meta.Leader())
	if err != nil {
		return err
	}

	cc := pb.NewPoleClient(client)

	rs, err := cc.Lock(context.Background(), &pb.LockRequest{LockUri: lockUri})
	if err != nil {
		return err
	}
	if rs.Code != 0 {
		return fmt.Errorf("lock failed: %s", rs.Message)
	}
	return nil
}

func (p *Poled) unlockByGrpc(lockUri string) error {
	client, err := poleRaft.GetClientConn(p.meta.Leader())
	if err != nil {
		return err
	}

	cc := pb.NewPoleClient(client)

	rs, err := cc.Unlock(context.Background(), &pb.UnlockRequest{LockUri: lockUri})
	if err != nil {
		return err
	}
	if rs.Code != 0 {
		return fmt.Errorf("lock failed: %s", rs.Message)
	}
	return nil
}
