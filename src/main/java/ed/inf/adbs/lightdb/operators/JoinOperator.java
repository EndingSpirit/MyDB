package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.SelectExpressionDeParser;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator extends Operator {

    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression joinCondition;
    private final SelectExpressionDeParser expressionDeParser;
    private Tuple currentLeftTuple;  // Keep track of the current left tuple
    private Tuple currentRightTuple; // Keep track of the current right tuple

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, List<String> combinedSchema) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;

        // Assuming the SelectExpressionDeParser can handle tuples with combined schema
        this.expressionDeParser = new SelectExpressionDeParser(joinCondition, combinedSchema);
        this.currentLeftTuple = this.leftChild.getNextTuple();
        this.currentRightTuple = this.rightChild.getNextTuple();
    }

    @Override
    public Tuple getNextTuple() {
        // 检查当前左元组是否为空，如果是，则尝试获取下一个左元组
        if (currentLeftTuple == null) {
            currentLeftTuple = leftChild.getNextTuple();
            // 如果左子操作符没有更多元组，则返回null
            if (currentLeftTuple == null) {
                return null;
            }
        }
        do {
            // 尝试与右子操作符的每个元组进行匹配
            while (currentRightTuple != null) {
                Tuple combinedTuple = combineTuples(currentLeftTuple, currentRightTuple);
                if (joinCondition == null || expressionDeParser.evaluate(combinedTuple)) {
                    // 如果找到匹配的组合，则准备返回并尝试获取下一个右元组
                    currentRightTuple = rightChild.getNextTuple();
                    return combinedTuple;
                }
                // 如果当前右元组不匹配，尝试获取下一个右元组
                currentRightTuple = rightChild.getNextTuple();
            }
            // 当右子操作符的所有元组都已尝试后，重置并获取下一个左元组
            rightChild.reset();
            currentRightTuple = rightChild.getNextTuple();
            currentLeftTuple = leftChild.getNextTuple();
        } while (currentLeftTuple != null);

        return null; // 当左子操作符没有更多元组时结束
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        currentLeftTuple = null;
        currentRightTuple = null;
    }

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<Integer> combinedFields = new ArrayList<>(leftTuple.getFields());
        combinedFields.addAll(rightTuple.getFields());
        return new Tuple(combinedFields);
    }

}
