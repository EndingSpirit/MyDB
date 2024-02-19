package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.List;

/**
 * SelectExpressionDeParser is used to evaluate the where condition
 */
public class SelectExpressionDeParser extends ExpressionDeParser {
    private Expression expression;
    private Tuple currentTuple;
    private List<String> schema;
    private Boolean result = null; // Use Boolean to handle null (uninitialized)

    /**
     * Constructor for SelectExpressionDeParser
     * @param expression The where condition
     * @param schema The schema of the table
     */
    public SelectExpressionDeParser(Expression expression, List<String> schema) {
        super();
        this.schema = schema;
        this.expression = expression;
    }

    /**
     * Evaluate the where condition for the given tuple
     * @param tuple The tuple to be evaluated
     * @return true if the tuple satisfies the where condition, false otherwise
     */
    public boolean evaluate(Tuple tuple) {
        this.currentTuple = tuple;
        this.result = true; // Default to true, will be updated by expression evaluation
        expression.accept(this);
        return result != null && result; // Handle null as false
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        Integer leftValue = getValue(equalsTo.getLeftExpression());
        Integer rightValue = getValue(equalsTo.getRightExpression());
        result = leftValue != null && leftValue.equals(rightValue);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        Integer leftValue = getValue(greaterThan.getLeftExpression());
        Integer rightValue = getValue(greaterThan.getRightExpression());
        result = leftValue != null && rightValue != null && leftValue > rightValue;
    }

    @Override
    public void visit(MinorThan minorThanEquals) {
        super.visit(minorThanEquals);
        Integer leftValue = getValue(minorThanEquals.getLeftExpression());
        Integer rightValue = getValue(minorThanEquals.getRightExpression());
        result = leftValue != null && rightValue != null && leftValue < rightValue;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        Integer leftValue = getValue(notEqualsTo.getLeftExpression());
        Integer rightValue = getValue(notEqualsTo.getRightExpression());
        result = leftValue != null && rightValue != null && !leftValue.equals(rightValue);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        Integer leftValue = getValue(greaterThanEquals.getLeftExpression());
        Integer rightValue = getValue(greaterThanEquals.getRightExpression());
        result = leftValue != null && rightValue != null && leftValue >= rightValue;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        Integer leftValue = getValue(minorThanEquals.getLeftExpression());
        Integer rightValue = getValue(minorThanEquals.getRightExpression());
        result = leftValue != null && rightValue != null && leftValue <= rightValue;
    }

    @Override
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
        andExpression.getLeftExpression().accept(this);
        Boolean leftResult = result;
        andExpression.getRightExpression().accept(this);
        result = leftResult != null && result != null && leftResult && result;
    }
    @Override
    public void visit(OrExpression orExpression) {
        throw new UnsupportedOperationException("OR expressions are not supported.");
    }

    @Override
    public void visit(Between between) {
        throw new UnsupportedOperationException("BETWEEN expressions are not supported.");
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new UnsupportedOperationException("IN expressions are not supported.");
    }


    private Integer getValue(Expression expression) {
        if (expression instanceof Column) {
            String columnName = ((Column) expression).getColumnName();
            int index = schema.indexOf(columnName);
            if (index != -1) {
                return currentTuple.getField(index);
            }
        } else if (expression instanceof LongValue) {
            return (int) ((LongValue) expression).getValue();
        }
        // Add more cases here for other expression types as needed
        return null;
    }


}
