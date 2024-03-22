package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Catalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProjectionOperator extends Operator {

    private final List<SelectItem<?>> selectItems;
    private final Operator child;

    private final List<String> schema;

    public ProjectionOperator(Operator child, PlainSelect plainSelect) {
        this.child = child;
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        this.schema = Catalog.getInstance().getSchemasFromPlain(plainSelect);

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
        for (SelectItem<?> item : this.selectItems) {
            Expression expression = item.getExpression();
            if (expression instanceof Column) {
                Column column = (Column) expression;
                String columnName;
                if (Config.getInstance().isUseAliases()) {
                    // 如果使用了别名，就用包含别名的完整列名
                    columnName = column.getFullyQualifiedName();
                } else {
                    // 如果没有使用别名，只用列名
                    columnName = column.getColumnName();
                }
                int index = schema.indexOf(columnName);
                if (index != -1) {
                    newFields.add(tuple.getField(index));
                } else {
                    // 如果列名在schema中找不到，那么我们应该抛出一个异常或者返回一个错误
                    throw new RuntimeException("Column not found in schema: " + columnName);
                }
            }
        }

        return new Tuple(newFields);
    }



    @Override
    public void reset(){
        this.child.reset();
    }
}
