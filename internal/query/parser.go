package query

import (
	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/xwb1989/sqlparser"
)

type Parser struct {
	input string
}

func NewParser(input string) *Parser {
	return &Parser{input}
}

func (t *Parser) Build() (Query, error) {
	stmt, err := sqlparser.Parse(t.input)
	if err != nil {
		panic(err)
	}

	return parse(stmt)

}

func parse(stmt sqlparser.Statement) (Query, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		columns := []string{}

		for _, expr := range stmt.SelectExprs {
			switch expr := expr.(type) {
			case *sqlparser.AliasedExpr:
				switch col := expr.Expr.(type) {
				case *sqlparser.ColName:
					columns = append(columns, col.Name.String())
				default:
					columns = append(columns, sqlparser.String(expr.Expr))
				}
			}
		}

		tables := []string{}
		for _, tblExpr := range stmt.From {
			switch tbl := tblExpr.(type) {
			case *sqlparser.AliasedTableExpr:
				switch expr := tbl.Expr.(type) {
				case sqlparser.TableName:
					tables = append(tables, expr.Name.String())
				}
			}
		}

		whereClause := &WhereAST{ast: buildAST(stmt.Where.Expr)}
		return SelectOp{
			Table:   tables[0],
			Columns: columns,
			where:   whereClause,
		}, nil

	case *sqlparser.Insert:
		values := make([]ColumnVal, len(stmt.Columns))
		for i, col := range stmt.Columns {
			values[i] = ColumnVal{Name: col.String()}
		}
		for _, row := range stmt.Rows.(sqlparser.Values) {
			for i, val := range row {
				values[i].Val = sqlparser.String(val)
			}
		}

		op := InsertOp{
			Table:  stmt.Table.Name.String(),
			Values: values,
		}

		return op, nil

	case *sqlparser.DDL:
		schema := make([]ColumnSchema, 0)
		if stmt.Action == sqlparser.CreateStr {
			tableName := stmt.NewName.Name.String()
			for _, col := range stmt.TableSpec.Columns {
				schema = append(schema, ColumnSchema{
					Name: col.Name.String(),
					Type: col.Type.Type,
				})
			}

			return CreateOp{
				Table:  tableName,
				Schema: schema,
			}, nil
		}

		return nil, errors.SQLParseError

	default:
		return nil, errors.SQLParseError
	}
}

func buildAST(expr sqlparser.Expr) *AstNode {
	switch node := expr.(type) {
	case *sqlparser.AndExpr:
		return &AstNode{Op: string(T_AND), SubOp1: buildAST(node.Left), SubOp2: buildAST(node.Right)}

	case *sqlparser.OrExpr:

		return &AstNode{Op: string(T_OR), SubOp1: buildAST(node.Left), SubOp2: buildAST(node.Right)}

	case *sqlparser.ComparisonExpr:
		var op string
		switch node.Operator {
		case sqlparser.EqualStr:
			op = string(T_EQUALS)
		case sqlparser.NotEqualStr:
			op = string(T_NOTEQUALS)
		case sqlparser.LessThanStr:
			op = string(T_LESSTHAN)
		case sqlparser.GreaterThanStr:
			op = string(T_GREATERTHAN)
		case sqlparser.LessEqualStr:
			op = string(T_LESSTHANOREQALS)
		case sqlparser.GreaterEqualStr:
			op = string(T_GREATERTHANOREQUALS)
		}

		return &AstNode{Op: op, SubOp1: buildAST(node.Left), SubOp2: buildAST(node.Right)}

	case *sqlparser.ParenExpr:
		return buildAST(node.Expr)

	case *sqlparser.ColName:
		return &AstNode{ColumnName: node.Name.String()}

	case *sqlparser.SQLVal:
		return &AstNode{Value: string(node.Val)}
	}

	return nil
}
