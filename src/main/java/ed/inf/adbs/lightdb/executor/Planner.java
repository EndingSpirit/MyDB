package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.utils.Catlog;
import ed.inf.adbs.lightdb.utils.JoinExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.ArrayList;
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
        Catlog catalog = Catlog.getInstance();
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan = new ScanOperator(tableName);

        // Extract join and selection conditions from WHERE clause
        JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
        if (where != null) {
            where.accept(joinExpressionDeParser);
        }

        // Apply selection conditions related to the FROM table before the JOINs
        for (Expression expr : joinExpressionDeParser.getSelectionConditions()) {
            queryPlan = new SelectOperator(queryPlan, expr, tableName);
        }


        // Handle the joins
        if (joins != null && !joins.isEmpty()) {
            for (Join join : joins) {
                String rightTableName = join.getRightItem().toString();
                Operator rightTableScan = new ScanOperator(rightTableName);
                List<String> leftSchema = catalog.getTableSchema(tableName);
                List<String> rightSchema = catalog.getTableSchema(rightTableName);

                Expression joinCondition = combineJoinConditions((List<Expression>) join.getOnExpressions(), joinExpressionDeParser.getJoinConditions());

                // Create the JoinOperator
                queryPlan = new JoinOperator(queryPlan, rightTableScan, joinCondition, leftSchema, rightSchema);

                // For subsequent joins, consider the combined schema
                tableName = ""; // Placeholder for combined table name/alias
            }
        }

        // Finally, apply projection
        if (plainSelect.getSelectItems() != null) {
            queryPlan = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
        }

        return queryPlan;
    }

    private static Expression combineJoinConditions(List<Expression> onExpressions, List<Expression> joinConditions) {
        // If there are explicit ON expressions in the JOIN, they take precedence
        if (onExpressions != null && !onExpressions.isEmpty()) {
            return onExpressions.get(0); // Assuming only one ON expression for simplicity
        }
        // Otherwise, we use join conditions extracted from WHERE clause
        if (joinConditions != null && !joinConditions.isEmpty()) {
            // Assuming all join conditions are combined with AND
            Expression combined = joinConditions.get(0);
            for (int i = 1; i < joinConditions.size(); i++) {
                combined = new AndExpression(combined, joinConditions.get(i));
            }
            return combined;
        }
        return null; // If no join conditions, it's a cross product
    }
}

