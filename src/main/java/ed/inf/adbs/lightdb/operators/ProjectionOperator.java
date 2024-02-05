package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProjectionOperator extends Operator {

    private Operator child;
    private List<Integer> projectionIndexes;

    public ProjectionOperator(Operator child, List<SelectItem<?>> selectItems) {
        this.child = child;
        this.projectionIndexes = new ArrayList<>();
    }

    @Override
    public Tuple getNextTuple() throws IOException {
        Tuple tuple = child.getNextTuple();
        if (tuple == null) {
            return null;
        }

        List<Integer> projectedFields = new ArrayList<>();
        for (Integer index : projectionIndexes) {
            projectedFields.add(tuple.getField(index));
        }
        return new Tuple(projectedFields);
    }

    @Override
    public void reset() throws IOException {
        child.reset();
    }
}
