package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Catalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * ProjectionOperator is used to project the columns in the select clause
 */
public class ProjectionOperator extends Operator {

    private final List<SelectItem<?>> selectItems;
    private final Operator child;

    private final List<String> schema;

    /**
     * Constructor for ProjectionOperator
     * @param child The child operator
     * @param plainSelect The select clause
     */
    public ProjectionOperator(Operator child, PlainSelect plainSelect) {
        this.child = child;
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        if (child instanceof SumOperator) {
            // If the ProjectionOperator's child is a SumOperator, we need to pass the schema of the SumOperator to the ProjectionOperator
            this.schema = ((SumOperator) child).groupBySchema;
        } else {
            // If the ProjectionOperator's child is not SumOperator, then we need to get the schema from the Catalog
            this.schema = Catalog.getInstance().getSchemasFromPlain(plainSelect);
        }

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
                // Get the column name and handle the case when aliases are used
                String columnName = Config.getInstance().isUseAliases() ? column.getFullyQualifiedName() : column.getColumnName();
                int index = schema.indexOf(columnName);
                if (index != -1) {
                    newFields.add(tuple.getField(index));
                } else {
                    throw new RuntimeException("Column not found in schema: " + columnName);
                }
            } else if (expression instanceof Function){
                Function function = (Function) expression;
                String functionName = function.getName();
                if (functionName.equals("SUM")) {
                    newFields.add(tuple.getField(tuple.getFields().size() - 1));
                } else {
                    throw new RuntimeException("Unsupported function: " + functionName);
                }
            } else {
                throw new RuntimeException("Unsupported select item: " + expression);
            }
        }
        return new Tuple(newFields);
    }

    @Override
    public void reset(){
        this.child.reset();
    }
}
