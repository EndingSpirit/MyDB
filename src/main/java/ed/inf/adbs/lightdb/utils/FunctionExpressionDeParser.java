package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

public class FunctionExpressionDeParser extends ExpressionDeParser {
    private final List<String> schema;
    private Tuple currentTuple;
    private final Expression expression;
    private Integer result;

    public FunctionExpressionDeParser(Expression expression, List<String> schema) {
        super();
        this.schema = schema;
        this.expression = expression;
    }

    public Integer evaluate(Tuple tuple) {
        this.currentTuple = tuple;
        this.result = null; // Default to true, will be updated by expression evaluation
        expression.accept(this);
        return result;
    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        Integer left = this.result;
        addition.getRightExpression().accept(this);
        Integer right = this.result;
        if (left != null && right != null) {
            this.result = left + right;
        } else {
            this.result = null;
        }
    }

    @Override
    public void visit(Subtraction subtraction) {
        subtraction.getLeftExpression().accept(this);
        Integer left = this.result;
        subtraction.getRightExpression().accept(this);
        Integer right = this.result;
        if (left != null && right != null) {
            this.result = left - right;
        } else {
            this.result = null;
        }
    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        Integer left = this.result;
        multiplication.getRightExpression().accept(this);
        Integer right = this.result;
        if (left != null && right != null) {
            this.result = left * right;
        } else {
            this.result = null;
        }
    }

    @Override
    public void visit(Division division) {
        division.getLeftExpression().accept(this);
        Integer left = this.result;
        division.getRightExpression().accept(this);
        Integer right = this.result;
        if (left != null && right != null && right != 0) {
            this.result = left / right;
        } else {
            this.result = null;
        }
    }

    @Override
    public void visit(Column column) {
        String columnName = column.getColumnName();
        int columnIndex = schema.indexOf(columnName);
        if (columnIndex != -1) {
            this.result = currentTuple.getField(columnIndex);
        } else {
            // Column not found in schema, handle accordingly
            this.result = null;
        }
    }
    @Override
    public void visit(LongValue longValue) {
        this.result = (int) longValue.getValue();
    }

}

