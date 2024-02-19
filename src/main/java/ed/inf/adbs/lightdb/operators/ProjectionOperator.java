package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Catlog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProjectionOperator extends Operator {

    private final List<SelectItem<?>> selectItems;
    private Operator child;
    private PlainSelect plainSelect;

    private List<String> schema;

    public ProjectionOperator(Operator child, List<SelectItem<?>> selectItems,String tableName) {
        this.child = child;
        this.plainSelect = plainSelect;
        this.schema = Catlog.getInstance().getTableSchema(tableName);
        this.selectItems = selectItems;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = this.child.getNextTuple();
        if (tuple == null) {
            return null;
        }
        if (this.selectItems.get(0).toString().equals("*")) {
            return tuple;
        }

        List<Integer> newFields = new ArrayList<>();
        for (SelectItem item : this.selectItems) {
                Expression expression = item.getExpression();
                if (expression instanceof Column) {
                    String columnName = ((Column) expression).getColumnName();
                    int index = this.schema.indexOf(columnName);
                    if (index != -1) {
                        newFields.add(tuple.getField(index));
                    }
                }
            }
        return new Tuple(newFields);
    }


    @Override
    public void reset() throws IOException {
        this.child.reset();
    }
}
