package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class ExpressionEvaluator extends ExpressionDeParser {
    private Tuple tuple;
    private boolean result;

    public ExpressionEvaluator(Tuple tuple) {
        this.tuple = tuple;
    }

    public boolean getResult() {
        return result;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        // 假设所有值都是整数类型
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();

        if (leftExpression instanceof Column && rightExpression instanceof LongValue) {
            String columnName = ((Column) leftExpression).getColumnName();
            int columnValue = Integer.parseInt(tuple.getField(columnName).toString()); // 获取列值
            long expectedValue = ((LongValue) rightExpression).getValue();
            result = columnValue == expectedValue;
        }

        // 为其他类型的 Expression 添加更多逻辑
    }

    // 根据需要重写其他访问方法
}

