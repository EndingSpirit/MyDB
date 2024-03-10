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
import java.util.Arrays;
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
        Operator queryPlan = null;

        if (joins != null && !joins.isEmpty()) {
        // Extract join and selection conditions from WHERE clause
        JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();
        if (where != null) {
            where.accept(joinExpressionDeParser);
        }
        List<String> tableNames = new ArrayList<>();
        tableNames.add(plainSelect.getFromItem().toString());
        if (joins != null) {
            for (Join join : joins) {
                tableNames.add(join.toString());
            }
        }

        Operator previousOperator = null;
        List<String> previousSchema = null;

        for (int i = 0; i < tableNames.size(); i++) {
            String table = tableNames.get(i);
            List<Expression> conditions = new ArrayList<>();
            for (Expression expression : joinExpressionDeParser.getSelectionConditions()) {
                List<String> l = getTableName(expression);
                if (l.size() == 0) {
                    if (i == 0) { // 仅对第一个表应用此逻辑
                        conditions.add(expression);
                    }
                } else if (table.equals(l.get(0))) {
                    conditions.add(expression);
                }
            }
            Expression expression = combineJoinConditions(conditions);
            SelectOperator selectOperator = new SelectOperator(new ScanOperator(table),expression, table);

            if (i == 0) {
                previousOperator = selectOperator; // 初始选择操作作为第一个操作
                previousSchema = catalog.getTableSchema(table);
            } else {
                String right = tableNames.get(i);
                List<String> rightSchema = catalog.getTableSchema(right);

                List<String> schema = new ArrayList<>(previousSchema);
                schema.addAll(rightSchema);

                Expression joinExpression = getAndMergeExpressions(tableNames.subList(0, i), right, tableNames, joinExpressionDeParser.getJoinConditions());

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

    private static Expression combineJoinConditions(List<Expression> joinConditions) {
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
    private static List<String> getTableName(Expression expression) {
        List<String> tableNames = new ArrayList<>();
        // 使用 AND 来分割表达式
        String[] expressions = expression.toString().split("AND");

        for (String exp : expressions) {
            // 分别提取表达式的两侧
            String[] parts = exp.split("=|!=|>|>=|<|<=");
            for (String part : parts) {
                String tableName = extractTableName(part.trim());
                if (tableName != null && !tableNames.contains(tableName)) {
                    tableNames.add(tableName);
                }
            }
        }
        return tableNames;
    }
    // 辅助方法，用于提取表达式中的表名
    private static String extractTableName(String expressionPart) {
        // 检查是否为数字，如果不是则尝试提取表名
        if (!isInteger(expressionPart)) {
            return expressionPart.split("\\.")[0];
        }
        return null;
    }

    // 检查字符串是否为整数
    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public static Expression getAndMergeExpressions(List<String> left, String right, List<String> tableNameList, List<Expression> joinConditionList) {
        List<Expression> conditions = new ArrayList<>();
        List<String> l = adjustTableNameOrder(tableNameList);

        for (Expression expression : joinConditionList) {
            List<String> sortedTableNames = getSortedTableNameFromExpression(expression, l);
            for (String s : left) {
                if (isConditionMatch(s, right, sortedTableNames)) {
                    conditions.add(expression);
                    break;
                }
            }
        }

        return mergeExpressionsWithAnd(conditions);
    }

    private static List<String> adjustTableNameOrder(List<String> tableNameList) {
        List<String> adjustedList = new ArrayList<>();
        for (String tableName : tableNameList) {
            String[] split = tableName.split(" ");
            adjustedList.add(split[split.length - 1]);
        }
        return adjustedList;
    }

    private static boolean isConditionMatch(String left, String right, List<String> sortedTableNames) {
        String[] l = left.split(" ");
        String[] r = right.split(" ");
        return l[l.length - 1].equals(sortedTableNames.get(0)) && r[r.length - 1].equals(sortedTableNames.get(1));
    }

    private static Expression mergeExpressionsWithAnd(List<Expression> conditions) {
        Expression expression = null;
        if (!conditions.isEmpty()) {
            expression = conditions.get(0);
            for (int i = 1; i < conditions.size(); i++) {
                expression = new AndExpression(expression, conditions.get(i));
            }
        }
        return expression;
    }

    private static List<String> getSortedTableNameFromExpression(Expression expression, List<String> adjustedList) {
        List<String> res = getTableName(expression);
        if (adjustedList.indexOf(res.get(0)) > adjustedList.indexOf(res.get(1))) {
            String temp = res.get(0);
            res.set(0, res.get(1));
            res.set(1, temp);
        }
        return res;
    }
}


