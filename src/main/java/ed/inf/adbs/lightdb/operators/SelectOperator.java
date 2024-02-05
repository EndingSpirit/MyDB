package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.Config;
import net.sf.jsqlparser.expression.Expression;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectOperator extends Operator {

    private Operator child; // ScanOperator 作为子操作符
    private Expression whereCondition; // 选择条件

    public SelectOperator(Operator child, Expression whereCondition) {
        this.child = child;
        this.whereCondition = whereCondition;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            if (evaluateTuple(tuple, whereCondition)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        child.reset();
    }

    private boolean evaluateTuple(Tuple tuple, Expression expression) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple);
        expression.accept(evaluator);
        return evaluator.getResult();
    }
}

