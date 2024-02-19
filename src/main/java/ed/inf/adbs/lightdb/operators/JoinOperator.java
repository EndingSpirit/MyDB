package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.SelectExpressionDeParser;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator extends Operator {

    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private SelectExpressionDeParser expressionDeParser;
    private Tuple currentLeftTuple = null;  // Keep track of the current left tuple
    private Tuple currentRightTuple = null; // Keep track of the current right tuple

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, List<String> leftSchema, List<String> rightSchema) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;

        // Assuming the SelectExpressionDeParser can handle tuples with combined schema
        this.expressionDeParser = new SelectExpressionDeParser(joinCondition, combineSchemas(leftSchema, rightSchema));
    }

    @Override
    public Tuple getNextTuple() {
        if (currentLeftTuple == null) {
            currentLeftTuple = leftChild.getNextTuple();
        }

        while (currentLeftTuple != null) {
            while ((currentRightTuple = rightChild.getNextTuple()) != null) {
                Tuple combinedTuple = combineTuples(currentLeftTuple, currentRightTuple);
                if (joinCondition == null || expressionDeParser.evaluate(combinedTuple)) {
                    return combinedTuple;  // Return the matched combined tuple
                }
            }
            currentLeftTuple = leftChild.getNextTuple();
            rightChild.reset();
        }
        return null;
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

    private List<String> combineSchemas(List<String> leftSchema, List<String> rightSchema) {
        List<String> combinedSchema = new ArrayList<>(leftSchema);
        combinedSchema.addAll(rightSchema);
        return combinedSchema;
    }

}
