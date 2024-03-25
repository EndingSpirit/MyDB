package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

/**
 * SelectExpressionDeParser is used to evaluate the where condition
 */
public class SelectExpressionDeParser extends ExpressionDeParser {
    private final Expression expression;
    private Tuple currentTuple;
    private final List<String> schema;
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
        Integer leftValue = getValue(equalsTo.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(equalsTo.getRightExpression(), schema, currentTuple);
        result = leftValue != null && leftValue.equals(rightValue);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        Integer leftValue = getValue(greaterThan.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(greaterThan.getRightExpression(), schema, currentTuple);
        result = leftValue != null && rightValue != null && leftValue > rightValue;
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        Integer leftValue = getValue(minorThan.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(minorThan.getRightExpression(), schema, currentTuple);
        result = leftValue != null && rightValue != null && leftValue < rightValue;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        Integer leftValue = getValue(notEqualsTo.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(notEqualsTo.getRightExpression(), schema, currentTuple);
        result = leftValue != null && rightValue != null && !leftValue.equals(rightValue);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        Integer leftValue = getValue(greaterThanEquals.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(greaterThanEquals.getRightExpression(), schema, currentTuple);
        result = leftValue != null && rightValue != null && leftValue >= rightValue;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        Integer leftValue = getValue(minorThanEquals.getLeftExpression(), schema, currentTuple);
        Integer rightValue = getValue(minorThanEquals.getRightExpression(), schema, currentTuple);
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


    /**
     * Get the value of the expression
     * @param expression The expression to be evaluated
     * @param schema The schema of the table
     * @param currentTuple The tuple to be evaluated
     * @return The value of the expression
     */
    private Integer getValue(Expression expression, List<String> schema, Tuple currentTuple) {
        if (expression instanceof Column) {
            String fullColumnName = ((Column) expression).getFullyQualifiedName();
            int index = schema.indexOf(fullColumnName);
            if (index != -1) {
                return currentTuple.getField(index);
            } else {
                String columnName = ((Column) expression).getColumnName();
                List<Integer> possibleIndexes = new ArrayList<>();
                for (int i = 0; i < schema.size(); i++) {
                    if (schema.get(i).endsWith("." + columnName) || schema.get(i).equals(columnName)) {
                        possibleIndexes.add(i);
                    }
                }
                if (possibleIndexes.size() == 1) {
                    // If only one match is found, it is safe to return the value of that column, whether an alias is used
                    return currentTuple.getField(possibleIndexes.get(0));
                } else if (possibleIndexes.isEmpty()) {
                    return null;
                } else {
                    throw new RuntimeException("Ambiguous column name without alias: " + columnName);
                }
            }
        } else if (expression instanceof LongValue) {
            return (int) ((LongValue) expression).getValue();
        }
        // Add more cases here for other expression types as needed
        return null;
    }




}
