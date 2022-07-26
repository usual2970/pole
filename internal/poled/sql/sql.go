package sql

import (
	"errors"
	"fmt"
	"sync"

	"pole/internal/poled/meta"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/parser/types"
)

type stmtType string

const (
	StmtTypeInsert stmtType = "insert"
	StmtTypeCreate stmtType = "create"
	StmtTypeDelete stmtType = "delete"
	StmtTypeDrop   stmtType = "drop"
	StmtTypeUpdate stmtType = "update"
	StmtTypeSelect stmtType = "select"
)

var (
	errDeleteCondition = errors.New("update operation's condition must be pattern 'id=xxx'")
)

const (
	defaultOffset = 0
	defaultLimit  = 10
)

var parserOnce = sync.Once{}
var sqlParser *parser.Parser

func getParser() *parser.Parser {
	parserOnce.Do(func() {
		sqlParser = parser.New()
	})
	return sqlParser
}

func Parse(sql string) (*SqlVistor, error) {
	p := getParser()
	nodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return extract(&nodes[0]), nil
}

type Col struct {
	Name string
	Typ  types.EvalType
}

type SqlVistor struct {
	ActionType    stmtType
	ColNames      []Col
	rows          []interface{}
	where         ast.Node
	SelectAll     bool
	TableName     string
	offset, limit int
	orderBy       []string
}

func (s *SqlVistor) docs(metas meta.Mapping) []*bluge.Document {
	columnCount := len(s.ColNames)
	var docs []*bluge.Document
	for i := 0; i < len(s.rows)/len(s.ColNames); i++ {
		var id string
		var fields []bluge.Field
		offset := columnCount * i
		for j := 0; j < columnCount; j++ {
			name := s.ColNames[j].Name
			value := s.rows[offset+j]
			if name == "id" {
				id = fmt.Sprintf("%v", value)
				continue
			}
			field, err := metas.MakeField(name, value)
			if field == nil || err != nil {
				continue
			}
			fields = append(fields, field)
		}
		if s.ActionType == StmtTypeUpdate {
			id, _ = s.getId()
		}

		doc := bluge.NewDocument(id)
		for _, field := range fields {
			doc.AddField(field)
		}
		docs = append(docs, doc)
	}
	return docs
}

func (s *SqlVistor) BuildInsertBatch(meta meta.Mapping) (*index.Batch, error) {
	if s.ActionType != StmtTypeInsert {
		return nil, errors.New("not insert operation")
	}
	batch := index.NewBatch()
	docs := s.docs(meta)
	for _, doc := range docs {
		batch.Update(doc.ID(), doc)
	}
	return batch, nil
}

func (s *SqlVistor) BuildUpdateBatch(meta meta.Mapping) (*index.Batch, error) {
	if s.ActionType != StmtTypeUpdate {
		return nil, errors.New("not update operation")
	}
	batch := index.NewBatch()

	docs := s.docs(meta)
	for _, doc := range docs {
		batch.Update(doc.ID(), doc)
	}
	return batch, nil
}

func (s *SqlVistor) BuildDeleteBatch(meta meta.Mapping) (*index.Batch, error) {
	if s.ActionType != StmtTypeDelete {
		return nil, errors.New("not delete operation")
	}
	batch := index.NewBatch()
	id, err := s.getId()
	if err != nil {
		return nil, err
	}
	batch.Delete(bluge.Identifier(id))

	return batch, nil
}

func (s *SqlVistor) BuildRequest(meta meta.Mapping) (bluge.SearchRequest, error) {

	var query bluge.Query
	if s.where == nil {
		query = bluge.NewMatchAllQuery()
	} else {
		visitor := NewBinaryOperationVisitor()
		s.where.Accept(visitor)
		var err error
		query, err = visitor.buildQuery(meta)
		if err != nil {
			return nil, err
		}
	}

	offset, limit := s.getPageInfo()

	req := bluge.NewTopNSearch(limit, query).WithStandardAggregations().
		IncludeLocations().
		SetFrom(offset).
		SortBy(s.orderBy).
		ExplainScores()
	return req, nil
}

func (s *SqlVistor) getId() (string, error) {
	if s.where == nil {
		return "", errDeleteCondition
	}
	where, ok := s.where.(*ast.BinaryOperationExpr)
	if !ok {
		return "", errDeleteCondition
	}
	if where.Op != opcode.EQ {
		return "", errDeleteCondition
	}

	columnName, ok := where.L.(*ast.ColumnNameExpr)
	if !ok {
		return "", errDeleteCondition
	}

	if columnName.Name.Name.O != "id" {
		return "", errDeleteCondition
	}

	value, ok := where.R.(*test_driver.ValueExpr)
	if !ok {
		return "", errDeleteCondition
	}

	return fmt.Sprintf("%v", value.GetValue()), nil
}

func (s *SqlVistor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.InsertStmt:
		s.ActionType = StmtTypeInsert
	case *ast.CreateTableStmt:
		s.ActionType = StmtTypeCreate
	case *ast.TableName:
		s.TableName = node.Name.O
	case *ast.ColumnDef:
		s.ColNames = append(s.ColNames, Col{
			Name: node.Name.Name.O,
			Typ:  node.Tp.EvalType(),
		})
		return in, true
	case *ast.ColumnName:
		s.ColNames = append(s.ColNames, Col{
			Name: node.Name.O,
			Typ:  types.ETInt,
		})
	case *test_driver.ValueExpr:
		s.rows = append(s.rows, node.GetValue())
	case *ast.DeleteStmt:
		s.ActionType = StmtTypeDelete
	case *ast.DropTableStmt:
		s.ActionType = StmtTypeDrop
	case *ast.UpdateStmt:
		s.ActionType = StmtTypeUpdate
	case *ast.BinaryOperationExpr, *ast.PatternInExpr, *ast.PatternLikeExpr:
		if s.TableName != "" {
			s.where = node
		}
		return in, true
	case *ast.SelectStmt:
		s.ActionType = StmtTypeSelect
	case *ast.FieldList:
		for _, field := range node.Fields {
			if field.WildCard != nil {
				s.SelectAll = true
				break
			}
			s.ColNames = append(s.ColNames, Col{
				Name: field.Expr.(*ast.ColumnNameExpr).Name.Name.O,
				Typ:  types.ETInt,
			})
		}
		return in, true
	case *ast.Limit:
		offset, ok := node.Offset.(*test_driver.ValueExpr)
		if ok {
			s.offset = int(offset.GetValue().(uint64))
		}
		limit, ok := node.Count.(*test_driver.ValueExpr)
		if ok {
			s.limit = int(limit.GetValue().(uint64))
		}
		return in, true
	case *ast.OrderByClause:
		orderBy := make([]string, 0, len(node.Items))
		for _, item := range node.Items {
			column := item.Expr.(*ast.ColumnNameExpr).Name.Name.O
			if column == "id" {
				column = meta.IdentifierField
			}
			if item.Desc {
				column = fmt.Sprintf("-%s", column)
			}
			orderBy = append(orderBy, column)
		}
		s.orderBy = orderBy
		return in, true
	}
	return in, false
}

func (s *SqlVistor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (s *SqlVistor) getPageInfo() (int, int) {
	if s.offset == 0 {
		s.offset = defaultOffset
	}
	if s.limit == 0 {
		s.limit = defaultLimit
	}
	return s.offset, s.limit
}

func extract(rootNode *ast.StmtNode) *SqlVistor {
	v := &SqlVistor{}
	(*rootNode).Accept(v)
	return v
}
