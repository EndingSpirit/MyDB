package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.Config;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;


public class SelectOperator extends Operator {

    private Operator child; // ScanOperator 作为子操作符
    private Expression whereCondition; // 选择条件
    private String tableName;

    public SelectOperator(Operator child, Expression whereCondition, String tableName) {
        this.child = child;
        this.whereCondition = whereCondition;
        this.tableName = tableName;
    }

    @Override
    public Tuple getNextTuple() throws IOException {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            if (evaluateTuple(tuple, whereCondition)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() throws IOException {
        child.reset();
    }

    private boolean evaluateTuple(Tuple tuple, Expression expression) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, tableName);
        expression.accept(evaluator);
        return evaluator.getResult();
    }
}

