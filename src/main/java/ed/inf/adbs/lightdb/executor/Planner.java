package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.JoinExpressionDeParser;
import ed.inf.adbs.lightdb.utils.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;

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
        Catalog catalog = Catalog.getInstance();
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan;

        if (joins != null && !joins.isEmpty()) {
            // Extract join and selection conditions from WHERE clause
            JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
            if (where != null) {
                where.accept(joinExpressionDeParser);
            }
            List<String> joinedTableNames = new ArrayList<>();
            joinedTableNames.add(plainSelect.getFromItem().toString());
                for (Join join : joins) {
                    joinedTableNames.add(join.toString());
                }

                Operator previousOperator = null;
            List<String> previousSchema = null;

            for (int i = 0; i < joinedTableNames.size(); i++) {
                String table = joinedTableNames.get(i);
                List<Expression> selectionConditions = new ArrayList<>();
                for (Expression expression : joinExpressionDeParser.getSelectionConditions()) {
                    List<String> l = SQLExpressionUtils.extractTableNamesFromExpression(expression);
                    if (l.isEmpty()) {
                        if (i == 0) { // Apply this logic only to the first table
                            selectionConditions.add(expression);
                        }
                    } else if (table.equals(l.get(0))) {
                        selectionConditions.add(expression);
                    }
                }
                Expression expression = SQLExpressionUtils.combineConditionsWithAnd(selectionConditions);
                SelectOperator selectOperator = new SelectOperator(new ScanOperator(table),expression, table);

                if (i == 0) {
                    previousOperator = selectOperator; // The initial selection operation is the first operation
                    previousSchema = catalog.getTableSchema(table);
                } else {
                    String right = joinedTableNames.get(i);
                    List<String> rightSchema = catalog.getTableSchema(right);

                    List<String> schema = new ArrayList<>(previousSchema);
                    schema.addAll(rightSchema);

                    Expression joinExpression = SQLExpressionUtils.mergeJoinConditionsForTables(joinedTableNames.subList(0, i), right, joinedTableNames, joinExpressionDeParser.getJoinConditions());

                    previousOperator = new JoinOperator(previousOperator, selectOperator,joinExpression, schema);
                    previousSchema = schema;
                }
            }

            queryPlan = previousOperator;
        } else {
            ScanOperator scanOperator = new ScanOperator(tableName);
            queryPlan = new SelectOperator(scanOperator, where, tableName);
        }

        ProjectionOperator projectionOperator = null;
        // Finally, apply projection
        if (plainSelect.getSelectItems() != null) {
            projectionOperator = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
        }

        queryPlan = projectionOperator;
        return queryPlan;
    }


}


