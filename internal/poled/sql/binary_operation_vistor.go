package sql

import (
	"container/list"
	"errors"
	"fmt"

	"github.com/blugelabs/bluge"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
)

var (
	ErrEqLeftMustBeColumn = errors.New("left must be column")
	ErrEqRightMustBeValue = errors.New("right must be value")
	ErrAndMustBeQuery     = errors.New("and must be query")
	ErrOrMustBeQuery      = errors.New("or must be query")
	ErrSyntaxNotSupported = errors.New("syntax not supported")
)

type BinaryOperationVisitor struct {
	prefixQueryNodes *list.List
}

func NewBinaryOperationVisitor() *BinaryOperationVisitor {
	return &BinaryOperationVisitor{
		prefixQueryNodes: list.New(),
	}
}

func (s *BinaryOperationVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.ParenthesesExpr, *ast.ColumnNameExpr:
		break
	default:
		s.prefixQueryNodes.PushBack(node)
	}
	return in, false
}

func (s *BinaryOperationVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (s *BinaryOperationVisitor) buildQuery() (bluge.Query, error) {
	calList := list.New()
	for s.prefixQueryNodes.Len() > 0 {
		back := s.prefixQueryNodes.Back()
		switch node := back.Value.(type) {
		case *ast.BinaryOperationExpr, *ast.PatternInExpr, *ast.PatternLikeExpr:
			node1 := calList.Back()
			node2 := node1.Prev()
			query, err := s.buildSingleQuery(node1.Value, node2.Value, back.Value)
			if err != nil {
				return nil, err
			}
			calList.Remove(node1)
			calList.Remove(node2)
			calList.PushBack(query)
		default:
			calList.PushBack(node)
		}
		s.prefixQueryNodes.Remove(back)
	}

	return calList.Back().Value.(bluge.Query), nil
}

func (s *BinaryOperationVisitor) buildSingleQuery(node1, node2 interface{}, expr interface{}) (bluge.Query, error) {
	var query bluge.Query
	switch expr := expr.(type) {
	case *ast.PatternLikeExpr:
		column, ok := node1.(*ast.ColumnName)
		if !ok {
			return nil, ErrEqLeftMustBeColumn
		}
		value, ok := node2.(*test_driver.ValueExpr)
		if !ok {
			return nil, ErrEqRightMustBeValue
		}
		query = bluge.NewWildcardQuery(fmt.Sprintf("%v", value.GetValue())).SetField(column.Name.O)
	case *ast.BinaryOperationExpr:
		switch expr.Op {
		case opcode.EQ:
			column, ok := node1.(*ast.ColumnName)
			if !ok {
				return nil, ErrEqLeftMustBeColumn
			}
			value, ok := node2.(*test_driver.ValueExpr)
			if !ok {
				return nil, ErrEqRightMustBeValue
			}
			query = bluge.NewMatchQuery(fmt.Sprintf("%v", value.GetValue())).SetField(column.Name.O)
		case opcode.LogicAnd:
			query1, ok := node1.(bluge.Query)
			if !ok {
				return nil, ErrEqLeftMustBeColumn
			}
			query2, ok := node2.(bluge.Query)
			if !ok {
				return nil, ErrEqRightMustBeValue
			}
			query = bluge.NewBooleanQuery().AddMust(query1, query2)
		case opcode.LogicOr:
			query1, ok := node1.(bluge.Query)
			if !ok {
				return nil, ErrAndMustBeQuery
			}
			query2, ok := node2.(bluge.Query)
			if !ok {
				return nil, ErrOrMustBeQuery
			}
			query = bluge.NewBooleanQuery().AddShould(query1, query2)
		default:
			return nil, ErrSyntaxNotSupported
		}

	}
	return query, nil
}
