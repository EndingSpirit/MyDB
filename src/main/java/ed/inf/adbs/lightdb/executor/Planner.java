package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.JoinExpressionDeParser;
import ed.inf.adbs.lightdb.utils.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        Boolean useAliases = plainSelect.getFromItem().getAlias() != null;
        Config config = Config.getInstance();
        config.setUseAliases(useAliases);

        if (plainSelect.getFromItem().getAlias() != null) {
            String alias = plainSelect.getFromItem().getAlias().getName();
            catalog.setTableAlias(alias, plainSelect.getFromItem().toString().split(" ")[0]);
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
                    catalog.setTableAlias(joinAlias, joinTableName.split(" ")[0]);
                    joinedTableNames.add(joinTableName);
                }else{
                    joinedTableNames.add(join.toString());
                }
            }

            queryPlan = processJoins(joinedTableNames, joinExpressionDeParser, catalog);
        } else {
            queryPlan = processSingleTable(tableName, where, catalog);
        }

        // Apply projection if needed
        ProjectionOperator projectionOperator = new ProjectionOperator(queryPlan, plainSelect);
        queryPlan = new SortOperator(plainSelect, projectionOperator);
        return queryPlan;
    }

    public static Operator processSingleTable(String tableName, Expression where, Catalog catalog) throws IOException {

        List<String> schema = catalog.getTableSchema(tableName);
        if (Config.getInstance().isUseAliases()) {
            String alias = tableName.contains(" ") ? tableName.split(" ")[1] : null;
            if (alias != null && !schema.get(0).contains(".")) {
                schema.replaceAll(s -> alias + "." + s);
            }
        }
        catalog.setAccumulatedSchema(tableName, new ArrayList<>(schema));
        // Now create the ScanOperator with the resolved table name
        ScanOperator scanOperator = new ScanOperator(tableName);

        // Proceed to apply selections, if any
        if (where != null) {
            return new SelectOperator(scanOperator, where, tableName,schema); // Ensure SelectOperator also uses the resolved name
        } else {
            return scanOperator;
        }
    }

    private static Operator processJoins(List<String> joinedTableNames, JoinExpressionDeParser joinExpressionDeParser, Catalog catalog) throws IOException {
        Operator previousOperator = null;
        List<String> accumulatedSchema = new ArrayList<>();

        for (int i = 0; i < joinedTableNames.size(); i++) {
            String table = joinedTableNames.get(i);

            String resolvedTableName = catalog.resolveTableName(table.split(" ")[0]);
            List<String> schema = new ArrayList<>(catalog.getTableSchema(resolvedTableName));

            if (Config.getInstance().isUseAliases()) {
                String alias = table.contains(" ") ? table.split(" ")[1] : null;
                if (alias != null && !schema.get(0).contains(".")) {
                    schema.replaceAll(s -> alias + "." + s);
                }
            }

            List<Expression> selectionConditions = new ArrayList<>();
            for (Expression expression : joinExpressionDeParser.getSelectionConditions()) {
                List<String> l = SQLExpressionUtils.extractTableNamesFromExpression(expression);
                String expressionTableName = catalog.getTableNameByAlias(l.get(0));
                if (Config.getInstance().isUseAliases()) {
                    if (l.isEmpty() && i == 0) {
                        selectionConditions.add(expression);
                    } else if (resolvedTableName.equals(expressionTableName)) {
                        selectionConditions.add(expression);
                    }
                }else {
                    if (l.isEmpty() && i == 0) {
                        selectionConditions.add(expression);
                    } else if (table.equals(l.get(0))) {
                        selectionConditions.add(expression);
                    }
                }
            }

            Expression combinedCondition = SQLExpressionUtils.combineConditionsWithAnd(selectionConditions);
            SelectOperator selectOperator = new SelectOperator(new ScanOperator(resolvedTableName), combinedCondition, resolvedTableName, schema);

            if (i == 0) {
                previousOperator = selectOperator;
                accumulatedSchema.addAll(schema);
                catalog.setAccumulatedSchema(table, new ArrayList<>(accumulatedSchema));
            } else {
                List<String> rightSchema = new ArrayList<>(catalog.getTableSchema(table));
                if (Config.getInstance().isUseAliases()) {
                    final String alias = table.contains(" ") ? table.split(" ")[1] : null;
                    if (alias != null && !rightSchema.get(0).contains(".")) {
                        rightSchema.replaceAll(s -> alias + "." + s);
                    }
                }
                accumulatedSchema.addAll(rightSchema);

                Expression joinExpression = SQLExpressionUtils.mergeJoinConditionsForTables(joinedTableNames.subList(0, i), table, joinedTableNames, joinExpressionDeParser.getJoinConditions());
                previousOperator = new JoinOperator(previousOperator, selectOperator, joinExpression, new ArrayList<>(accumulatedSchema));
                catalog.setAccumulatedSchema(table, new ArrayList<>(accumulatedSchema));
            }

        }

        return previousOperator;
    }

}
