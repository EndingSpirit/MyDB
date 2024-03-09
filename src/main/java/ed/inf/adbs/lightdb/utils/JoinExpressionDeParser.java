package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

public class JoinExpressionDeParser extends ExpressionDeParser {

    private List<Expression> joinConditions = new ArrayList<>();
    private List<Expression> selectionConditions = new ArrayList<>();


    public static Boolean isJoin(String left, String right) {
        String leftTable = left.contains(".") ? left.split("\\.")[0] : null;
        String rightTable = right.contains(".") ? right.split("\\.")[0] : null;

        return (leftTable != null && rightTable != null) && !leftTable.equals(rightTable);
    }
    public List<Expression> getJoinConditions() {
        return joinConditions;
    }
    public Expression getSpecificJoinCondition(String leftTable, String rightTable) {
        for (Expression expr : joinConditions) {
            if (expr instanceof EqualsTo) {
                EqualsTo equals = (EqualsTo) expr;
                String leftSideTable = ((Column) equals.getLeftExpression()).getTable().getName();
                String rightSideTable = ((Column) equals.getRightExpression()).getTable().getName();
                if (leftSideTable.equals(leftTable) && rightSideTable.equals(rightTable)) {
                    return expr;
                }
            }
            // 可以根据需要添加对其他 BinaryExpression 的处理
        }
        return null; // 如果没有找到特定的连接条件，返回 null
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
