package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.JoinExpressionDeParser;
import ed.inf.adbs.lightdb.utils.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

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
        // Get the joins, where condition, and all other relevant variables
        List<Join> joins = plainSelect.getJoins();
        Expression where = plainSelect.getWhere();
        Catalog catalog = Catalog.getInstance();
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan;
        Boolean useAliases = plainSelect.getFromItem().getAlias() != null;
        Config config = Config.getInstance();
        config.setUseAliases(useAliases);

        // Set the table alias if it exists
        if (plainSelect.getFromItem().getAlias() != null) {
            String alias = plainSelect.getFromItem().getAlias().getName();
            catalog.setTableAlias(alias, plainSelect.getFromItem().toString().split(" ")[0]);
        }

        // Process joins if they exist, otherwise process single table
        if (joins != null && !joins.isEmpty()) {
            // Create a JoinExpressionDeParser to extract join conditions
            JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
            if (where != null) {
                where.accept(joinExpressionDeParser);
            }
            List<String> joinedTableNames = new ArrayList<>();
            joinedTableNames.add(tableName);

            // Process each join, resolve table names, and create the join operator
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

        // Apply SUM and GROUP BY if needed
        List<Column> groupByAttributes = new ArrayList<>();
        GroupByElement groupBy = plainSelect.getGroupBy();
        if (groupBy != null) {
            ExpressionList<?> groupByExpressions = groupBy.getGroupByExpressions();
            if (groupByExpressions != null) {
                for (Object expr : groupByExpressions) {
                    if (expr instanceof Column) {
                        groupByAttributes.add(((Column) expr));
                    }
                }
            }
        }
        List<Expression> sumExpressions = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            // Check whether the expression is of type Function and SUM
            if (expr instanceof Function) {
                Function func = (Function) expr;
                if ("SUM".equalsIgnoreCase(func.getName())) {
                    sumExpressions.add(func);
                }
            }
        }

        // Apply SUM and GROUP BY if needed
        if (!sumExpressions.isEmpty() || !groupByAttributes.isEmpty()) {
            queryPlan = new SumOperator(queryPlan, sumExpressions, groupByAttributes, plainSelect);
        }

        // Apply projection
        queryPlan = new ProjectionOperator(queryPlan, plainSelect);

        // Apply ORDER BY
        queryPlan = new SortOperator(plainSelect, queryPlan);

        // Apply DISTINCT
        queryPlan = new DuplicateEliminationOperator(plainSelect, queryPlan);

        // Return the final operator of the query plan
        return queryPlan;
    }

    public static Operator processSingleTable(String tableName, Expression where, Catalog catalog) throws IOException {

        List<String> schema = catalog.getTableSchema(tableName);
        Catalog.resolveAliasSchema(tableName, schema);
        catalog.setAccumulatedSchema(tableName, new ArrayList<>(schema));
        // Now create the ScanOperator with the resolved table name
        ScanOperator scanOperator = new ScanOperator(tableName);

        // Proceed to apply selections, if any
        if (where != null) {
            return new SelectOperator(scanOperator, where,schema); // Ensure SelectOperator also uses the resolved name
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

            Catalog.resolveAliasSchema(table, schema);
            List<Expression> selectionConditions = new ArrayList<>();
            for (Expression expression : joinExpressionDeParser.getSelectionConditions()) {
                List<String> l = SQLExpressionUtils.extractTableNamesFromExpression(expression);

                // 对于没有表名的表达式，如1=1，应用于第一个表
                if (l.isEmpty() && i == 0) {
                    selectionConditions.add(expression);
                    continue;
                }
                // 对于包含表名的表达式
                for (String tableNameOrAlias : l) {
                    // 确保表达式仅应用于与之相关的表
                    if ((Config.getInstance().isUseAliases() && tableNameOrAlias.equals(table.split(" ")[1])) ||
                            (!Config.getInstance().isUseAliases() && table.equals(tableNameOrAlias))) {
                        selectionConditions.add(expression);
                        break; // 假设一个表达式不会同时针对多个表，一旦匹配成功，即可退出循环
                    }
                }
            }

            Expression combinedCondition = SQLExpressionUtils.combineConditionsWithAnd(selectionConditions);
            SelectOperator selectOperator = new SelectOperator(new ScanOperator(resolvedTableName), combinedCondition, schema);

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
