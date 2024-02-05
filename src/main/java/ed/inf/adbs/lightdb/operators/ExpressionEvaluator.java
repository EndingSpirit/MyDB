package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.DatabaseCatalog;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.Map;

public class ExpressionEvaluator extends ExpressionDeParser {
    private Tuple tuple;
    private boolean result;
    private Map<String, Integer> schema;

    public ExpressionEvaluator(Tuple tuple, String tableName) {
        this.tuple = tuple;
        this.schema = DatabaseCatalog.getInstance().getTableSchema(tableName);
    }

    public boolean getResult() {
        return result;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                String columnName = column.getColumnName();
                Integer index = schema.get(columnName);
                if (index != null) {
                    int columnValue = tuple.getField(index);
                    equalsTo.getRightExpression().accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(LongValue longValue) {
                            long expectedValue = longValue.getValue();
                            result = columnValue == expectedValue;
                        }
                    });
                }
            }
        });
    }

    // Implement other necessary methods and logic for handling different types of expressions
}


