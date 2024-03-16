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
 * Include the operator chain based on the SQL query.
 */
public class Planner {
    /**
     * Constructs the operator chain based on the SQL query.
     *
     * @param plainSelect The parsed representation of the select statement.
     * @return The root operator of the constructed query plan.
     * @throws IOException If an IO error occurs.
     */
    public static Operator constructQueryPlan(PlainSelect plainSelect) throws IOException {
        List<Join> joins = plainSelect.getJoins();
        Expression where = plainSelect.getWhere();
        Catalog catalog = Catalog.getInstance();
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan;

        if (plainSelect.getFromItem().getAlias() != null) {
            String alias = plainSelect.getFromItem().getAlias().getName();
            String actualTableName = plainSelect.getFromItem().toString().split(" ")[0]; // Ensure this is the correct way to get the table name
            catalog.setTableAlias(alias, actualTableName);
        }

        if (joins != null && !joins.isEmpty()) {
            JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
            if (where != null) {
                where.accept(joinExpressionDeParser);
            }
            List<String> joinedTableNames = new ArrayList<>();
            joinedTableNames.add(tableName);
            for (Join join : joins) {
                if (join.getRightItem().getAlias() != null) {
                    String joinAlias = join.getRightItem().getAlias().getName();
                    String joinTableName = join.getRightItem().toString();
                    Catalog.getInstance().setTableAlias(joinAlias, joinTableName);
                }else{
                    joinedTableNames.add(join.toString());
                }
            }

            queryPlan = processJoins(joinedTableNames, joinExpressionDeParser, catalog);
        } else {
            queryPlan = processSingleTable(tableName, where, catalog);
        }

        // Apply projection if needed
        if (plainSelect.getSelectItems() != null) {
            queryPlan = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
        }

        return queryPlan;
    }

    public static Operator processSingleTable(String tableName, Expression where, Catalog catalog) throws IOException {
        // Resolve the table name from an alias, if applicable
        String resolvedTableName = catalog.resolveTableName(tableName);

        // Now create the ScanOperator with the resolved table name
        ScanOperator scanOperator = new ScanOperator(resolvedTableName);

        // Proceed to apply selections, if any
        if (where != null) {
            return new SelectOperator(scanOperator, where, resolvedTableName); // Ensure SelectOperator also uses the resolved name
        } else {
            return scanOperator;
        }
    }

    private static Operator processJoins(List<String> joinedTableNames, JoinExpressionDeParser joinExpressionDeParser, Catalog catalog) throws IOException {
        Operator previousOperator = null;
        List<String> previousSchema = null;

        for (int i = 0; i < joinedTableNames.size(); i++) {
            String table = joinedTableNames.get(i);
            List<Expression> selectionConditions = new ArrayList<>();
            for (Expression expression : joinExpressionDeParser.getSelectionConditions()) {
                List<String> l = SQLExpressionUtils.extractTableNamesFromExpression(expression);
                if (l.isEmpty() && i == 0) {
                    selectionConditions.add(expression);
                } else if (table.equals(l.get(0))) {
                    selectionConditions.add(expression);
                }
            }

            Expression combinedCondition = SQLExpressionUtils.combineConditionsWithAnd(selectionConditions);
            SelectOperator selectOperator = new SelectOperator(new ScanOperator(table), combinedCondition, table);

            if (i == 0) {
                previousOperator = selectOperator;
                previousSchema = catalog.getTableSchema(table);
            } else {
                List<String> rightSchema = catalog.getTableSchema(table);
                List<String> schema = new ArrayList<>(previousSchema);
                schema.addAll(rightSchema);

                Expression joinExpression = SQLExpressionUtils.mergeJoinConditionsForTables(joinedTableNames.subList(0, i), table, joinedTableNames, joinExpressionDeParser.getJoinConditions());
                previousOperator = new JoinOperator(previousOperator, selectOperator, joinExpression, schema);
                previousSchema = schema;
            }
        }

        return previousOperator;
    }
}
