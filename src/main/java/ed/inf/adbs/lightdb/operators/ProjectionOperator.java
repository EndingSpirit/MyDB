package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Catlog;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.List;

public class ProjectionOperator extends Operator {

    private Operator child;
    private PlainSelect plainSelect;

    private List<String> schema;

    public ProjectionOperator(Operator child, List<SelectItem<?>> selectItems,String tableName) {
        this.child = child;
        this.plainSelect = plainSelect;
        this.schema = Catlog.getInstance().getTableSchema(plainSelect.getFromItem().toString());
    }

    @Override
    public Tuple getNextTuple(){
        Tuple tuple = this.child.getNextTuple();
        if (tuple == null) {
            return null;
        }

        if (this.plainSelect.getSelectItems().get(0).toString().equals("*")) {
            return tuple;
        }

        return tuple;
    }

    @Override
    public void reset() throws IOException {
        this.child.reset();
    }
}
