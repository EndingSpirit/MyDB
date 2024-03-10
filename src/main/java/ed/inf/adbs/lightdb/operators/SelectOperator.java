package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.SelectExpressionDeParser;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.lightdb.utils.Catalog;

import java.util.List;

/**
 * SelectOperator is used to filter the tuples based on the where condition
 */
public class SelectOperator extends Operator {

    private final Operator child; // ScanOperator as child
    private SelectExpressionDeParser selectExpressionDeParser;


    public SelectOperator(Operator child, Expression whereCondition, String tableName) {
        this.child = child;
        List<String> schema = Catalog.getInstance().getTableSchema(tableName);
        if (whereCondition != null) {
        this.selectExpressionDeParser = new SelectExpressionDeParser(whereCondition, schema);
        }
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

