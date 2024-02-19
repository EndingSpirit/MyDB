package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.utils.Catlog;
import ed.inf.adbs.lightdb.utils.JoinExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.List;

/**
 * The purpose of this class is to construct the query plan.
 * Include the operator chain based on the SQL query
 */
public class Planner {
    // Constructs the operator chain based on the SQL query
    public static Operator constructQueryPlan(PlainSelect plainSelect) throws IOException {
        List<Join> joins = plainSelect.getJoins();
        Expression where = plainSelect.getWhere();

        // Initialize the catalog to get table schema information
        Catlog catalog = Catlog.getInstance();

        // First, handle the main table (FROM clause)
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan = new ScanOperator(tableName);

        // If there's a WHERE clause, apply selection conditions
        if (where != null && (joins == null || joins.isEmpty())) {
            queryPlan = new SelectOperator(queryPlan, where, tableName);
        }

        // Now handle the joins
        if (joins != null && !joins.isEmpty()) {
            JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
            where.accept(joinExpressionDeParser);
            for (Join join : joins) {
                if (join.getRightItem() == null) {
                    throw new IllegalArgumentException("Join does not have a right item.");
                }

                String rightTableName = join.getRightItem().toString();
                Operator rightTableScan = new ScanOperator(rightTableName);

                // Prepare the schema for both tables in the join
                List<String> leftSchema = catalog.getTableSchema(tableName);
                List<String> rightSchema = catalog.getTableSchema(rightTableName);

                // Get the ON expressions for this join
                List<Expression> onExpressions = (List<Expression>) join.getOnExpressions();

                // You can have multiple ON expressions for a single JOIN
                // For the sake of this example, we only take the first one
                Expression onExpression = !onExpressions.isEmpty() ? onExpressions.get(0) :
                        joinExpressionDeParser.getJoinConditions().isEmpty() ? null :
                                joinExpressionDeParser.getJoinConditions().get(0);

                queryPlan = new JoinOperator(queryPlan, rightTableScan, onExpression, leftSchema, rightSchema);

                // Update the tableName to be the result of the join for subsequent joins
                // Implement your logic to handle table names or aliases here
            }
            for (Expression selectionCondition : joinExpressionDeParser.getSelectionConditions()) {
                queryPlan = new SelectOperator(queryPlan, selectionCondition, tableName);
            }
        } else if (where != null) {
            // 如果没有JOIN，只应用选择条件
            queryPlan = new SelectOperator(queryPlan, where, tableName);
        }

        // Finally, if there's a projection, apply it
        if (plainSelect.getSelectItems() != null) {
            queryPlan = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
        }

        return queryPlan;
    }
}
