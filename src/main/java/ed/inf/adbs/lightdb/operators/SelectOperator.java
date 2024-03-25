package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.SelectExpressionDeParser;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.List;

/**
 * SelectOperator is used to filter the tuples based on the where condition
 */
public class SelectOperator extends Operator {

    private final Operator child; // ScanOperator as child
    private final SelectExpressionDeParser selectExpressionDeParser;


    public SelectOperator(Operator child, Expression whereCondition,List<String> schema) {
        this.child = child;
        // Use the incoming schema instead of the internal parsing
        this.selectExpressionDeParser = whereCondition != null ? new SelectExpressionDeParser(whereCondition, schema) : null;
    }

    /**
     * Get the next tuple that satisfies the where condition
     * Use the selectExpressionDeParser to evaluate the where condition
     * @return the next tuple that satisfies the where condition
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = this.child.getNextTuple()) != null) {
            if (this.selectExpressionDeParser == null) {
                return tuple;
            }
            if (this.selectExpressionDeParser.evaluate(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void reset(){
        child.reset();
    }

}

