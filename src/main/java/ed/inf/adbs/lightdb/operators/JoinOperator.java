package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.SelectExpressionDeParser;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * JoinOperator is used to join two tables based on the join condition
 */
public class JoinOperator extends Operator {

    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression joinCondition;
    private final SelectExpressionDeParser expressionDeParser;
    private Tuple currentLeftTuple;  // Keep track of the current left tuple
    private Tuple currentRightTuple; // Keep track of the current right tuple

    /**
     * Constructor for JoinOperator
     * @param leftChild The left child operator
     * @param rightChild The right child operator
     * @param joinCondition The join condition
     * @param combinedSchema The schema of the combined table
     */
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
        // Check if the current left tuple is empty, and if so, try to get the next left tuple
        if (currentLeftTuple == null) {
            currentLeftTuple = leftChild.getNextTuple();
            // If the left operator has no more multiple groups, null is returned
            if (currentLeftTuple == null) {
                return null;
            }
        }
        do {
            // Try to match each tuple of the right child operator
            while (currentRightTuple != null) {
                Tuple combinedTuple = combineTuples(currentLeftTuple, currentRightTuple);
                if (joinCondition == null || expressionDeParser.evaluate(combinedTuple)) {
                    // If a matching combination is found, it is ready to go back and try to get the next right tuple
                    currentRightTuple = rightChild.getNextTuple();
                    return combinedTuple;
                }
                // If the current right tuple does not match, try to get the next right tuple
                currentRightTuple = rightChild.getNextTuple();
            }
            // When all tuples of the right operator have been tried, reset and get the next left tuple
            rightChild.reset();
            currentRightTuple = rightChild.getNextTuple();
            currentLeftTuple = leftChild.getNextTuple();
        } while (currentLeftTuple != null);

        return null; // Ends when the left operator has no more multiple groups
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        currentLeftTuple = null;
        currentRightTuple = null;
    }

    /**
     * Combine the fields of the left and right tuples
     * @param leftTuple The left tuple
     * @param rightTuple The right tuple
     * @return The combined tuple
     */
    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<Integer> combinedFields = new ArrayList<>(leftTuple.getFields());
        combinedFields.addAll(rightTuple.getFields());
        return new Tuple(combinedFields);
    }

}
