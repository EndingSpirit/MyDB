package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

public class JoinExpressionDeParser extends ExpressionDeParser {

    private final List<Expression> joinConditions = new ArrayList<>();
    private final List<Expression> selectionConditions = new ArrayList<>();


    public static Boolean isJoin(String left, String right) {
        String leftTable = left.contains(".") ? left.split("\\.")[0] : null;
        String rightTable = right.contains(".") ? right.split("\\.")[0] : null;

        return (leftTable != null && rightTable != null) && !leftTable.equals(rightTable);
    }
    public List<Expression> getJoinConditions() {
        return joinConditions;
    }
    public List<Expression> getSelectionConditions() {
        return selectionConditions;
    }
    private void handleBinaryExpression(BinaryExpression binaryExpression) {
        String left = binaryExpression.getLeftExpression().toString();
        String right = binaryExpression.getRightExpression().toString();
        if (isJoin(left, right)) {
            joinConditions.add(binaryExpression);
        } else {
            selectionConditions.add(binaryExpression);
        }
    }
    @Override
    public void visit(EqualsTo equalsTo) {
        handleBinaryExpression(equalsTo);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        handleBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        handleBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        handleBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        handleBinaryExpression(minorThan);
    }
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        handleBinaryExpression(minorThanEquals);
    }


}
