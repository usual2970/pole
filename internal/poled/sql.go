package poled

import (
	"errors"
	"fmt"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/parser/types"
	"github.com/rs/xid"
)

type stmtType string

const (
	stmtTypeInsert stmtType = "insert"
	stmtTypeCreate stmtType = "create"
	stmtTypeDelete stmtType = "delete"
	stmtTypeDrop   stmtType = "drop"
	stmtTypeUpdate stmtType = "update"
	stmtTypeSelect stmtType = "select"
)

var parserOnce = sync.Once{}
var sqlParser *parser.Parser

func getParser() *parser.Parser {
	parserOnce.Do(func() {
		sqlParser = parser.New()
	})
	return sqlParser
}

func parse(sql string) (*sqlRs, error) {
	p := getParser()
	nodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return extract(&nodes[0]), nil
}

type col struct {
	name string
	typ  types.EvalType
}

type sqlRs struct {
	actionType   stmtType
	colNames     []col
	rows         []interface{}
	where        bool
	selectAll    bool
	whereColumns []col
	whereRows    []interface{}
	tableName    string
}

func (s *sqlRs) docs(meta map[string]filedOptions) []*bluge.Document {
	columnCount := len(s.colNames)
	var docs []*bluge.Document
	for i := 0; i < len(s.rows)/len(s.colNames); i++ {
		var id string
		var fields []*bluge.TermField
		offset := columnCount * i
		for j := 0; j < columnCount; j++ {
			name := s.colNames[j].name
			option, ok := meta[name]
			if !ok {
				continue
			}
			value := s.rows[offset+j]
			if name == "id" {
				id = fmt.Sprintf("%v", value)
				continue
			}
			var field *bluge.TermField
			switch option.Type {
			case fieldTypeNumeric:
				field = bluge.NewNumericField(name, getNumericValue(value))

			case fieldTypeText:
				field = bluge.NewTextField(name, fmt.Sprintf("%v", value))
				field.FieldOptions = 3
			}
			if field == nil {
				continue
			}
			fields = append(fields, field)
		}

		for i, column := range s.whereColumns {
			if column.name == "id" {
				id = fmt.Sprintf("%v", s.whereRows[i])
			}
		}

		if id == "" {
			id = xid.New().String()
		}
		doc := bluge.NewDocument(id)
		for _, field := range fields {
			doc.AddField(field)
		}
		docs = append(docs, doc)
	}
	return docs
}

func (s *sqlRs) buildInsertBatch(meta map[string]filedOptions) (*index.Batch, error) {
	if s.actionType != stmtTypeInsert {
		return nil, errors.New("not insert operation")
	}
	batch := index.NewBatch()
	docs := s.docs(meta)
	for _, doc := range docs {
		batch.Update(doc.ID(), doc)
	}
	return batch, nil
}

func (s *sqlRs) buildUpdateBatch(meta map[string]filedOptions) (*index.Batch, error) {
	if s.actionType != stmtTypeUpdate {
		return nil, errors.New("not update operation")
	}
	batch := index.NewBatch()
	docs := s.docs(meta)
	for _, doc := range docs {
		batch.Update(doc.ID(), doc)
	}
	return batch, nil
}

func (s *sqlRs) buildDeleteBatch(meta map[string]filedOptions) (*index.Batch, error) {
	if s.actionType != stmtTypeDelete {
		return nil, errors.New("not delete operation")
	}
	batch := index.NewBatch()
	var id string
	for i, column := range s.whereColumns {
		if column.name == "id" {
			id = fmt.Sprintf("%v", s.whereRows[i])
		}
	}

	if id == "" {
		id = xid.New().String()
	}

	batch.Delete(bluge.Identifier(id))

	return batch, nil
}

func getNumericValue(value interface{}) float64 {
	switch v := value.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	}
	return 0
}

func (s *sqlRs) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.InsertStmt:
		s.actionType = stmtTypeInsert
	case *ast.CreateTableStmt:
		s.actionType = stmtTypeCreate
	case *ast.TableName:
		s.tableName = node.Name.O
	case *ast.ColumnDef:
		s.colNames = append(s.colNames, col{
			name: node.Name.Name.O,
			typ:  node.Tp.EvalType(),
		})
		return in, true
	case *ast.ColumnName:
		if s.where {
			s.whereColumns = append(s.whereColumns, col{
				name: node.Name.O,
				typ:  types.ETInt,
			})
			break
		}
		s.colNames = append(s.colNames, col{
			name: node.Name.O,
			typ:  types.ETInt,
		})
	case *test_driver.ValueExpr:
		if s.where {
			s.whereRows = append(s.whereRows, node.GetValue())
			break
		}
		s.rows = append(s.rows, node.GetValue())
	case *ast.DeleteStmt:
		s.actionType = stmtTypeDelete
	case *ast.DropTableStmt:
		s.actionType = stmtTypeDrop
	case *ast.UpdateStmt:
		s.actionType = stmtTypeUpdate
	case *ast.BinaryOperationExpr:
		s.where = true
	case *ast.SelectStmt:
		s.actionType = stmtTypeSelect
	case *ast.FieldList:
		for _, field := range node.Fields {
			if field.WildCard != nil {
				s.selectAll = true
				break
			}
			s.colNames = append(s.colNames, col{
				name: field.Expr.(*ast.ColumnNameExpr).Name.Name.O,
				typ:  types.ETInt,
			})
		}
		return in, true
	}
	return in, false
}

func (s *sqlRs) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extract(rootNode *ast.StmtNode) *sqlRs {
	v := &sqlRs{}
	(*rootNode).Accept(v)
	return v
}
