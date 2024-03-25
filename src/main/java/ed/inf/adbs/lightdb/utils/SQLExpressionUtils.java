package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * SQLExpressionUtils is used to handle the expression in the SQL
 */
public class SQLExpressionUtils {
    /**
     * Combine the conditions with AND
     * @param joinConditions the conditions to be combined
     * @return the combined expression
     */
    public static Expression combineConditionsWithAnd(List<Expression> joinConditions) {
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

    /**
     * Extract table names from the expression
     * @param expression the expression to be extracted
     * @return the list of table names
     */
    public static List<String> extractTableNamesFromExpression(Expression expression) {
        List<String> tableNames = new ArrayList<>();
        // Use AND to split the expression
        String[] expressions = expression.toString().split("AND");

        for (String exp : expressions) {
            // Extract both sides of the expression separately
            String[] parts = exp.split("=|!=|>|>=|<|<=");
            for (String part : parts) {
                String tableName = parseTableNameFromPart(part.trim());
                if (tableName != null && !tableNames.contains(tableName)) {
                    tableNames.add(tableName);
                }
            }
        }
        return tableNames;
    }

    /**Extracts table names from expressions
     * This method is used to support extractTableNamesFromExpression
     * @param expressionPart the expression part to be extracted
     * @return the table name
     */
    public static String parseTableNameFromPart(String expressionPart) {
        // Check if it is a number, and if it is not, try to extract the table name
        if (!isStringNumeric(expressionPart)) {
            return expressionPart.split("\\.")[0];
        }
        return null;
    }

    /**
     * Check if the string is an integer
     * @param str the string to be checked
     **/
    public static boolean isStringNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Merge join conditions for tables
     * @param left the left table name
     * @param right the right table name
     * @param tableNameList the list of table names
     * @param joinConditionList the list of join conditions
     * @return the merged expression
     */
    public static Expression mergeJoinConditionsForTables(List<String> left, String right, List<String> tableNameList, List<Expression> joinConditionList) {
        List<Expression> conditions = new ArrayList<>();
        List<String> l = normalizeTableNameOrder(tableNameList);

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

    /**
     * Normalize the table name order
     * @param tableNameList the list of table names
     * @return the adjusted list
     */
    public static List<String> normalizeTableNameOrder(List<String> tableNameList) {
        List<String> adjustedList = new ArrayList<>();
        for (String tableName : tableNameList) {
            String[] split = tableName.split(" ");
            adjustedList.add(split[split.length - 1]);
        }
        return adjustedList;
    }

    /**
     * Check if the condition matches
     * @param left the left expression
     * @param right the right expression
     * @param sortedTableNames the sorted table names
     * @return true if the condition matches, false otherwise
     */
    public static boolean isConditionMatch(String left, String right, List<String> sortedTableNames) {
        String[] l = left.split(" ");
        String[] r = right.split(" ");
        return l[l.length - 1].equals(sortedTableNames.get(0)) && r[r.length - 1].equals(sortedTableNames.get(1));
    }

    /**
     * Merge expressions with AND
     * @param conditions the conditions to be merged
     * @return the merged expression
     */
    public static Expression mergeExpressionsWithAnd(List<Expression> conditions) {
        Expression expression = null;
        if (!conditions.isEmpty()) {
            expression = conditions.get(0);
            for (int i = 1; i < conditions.size(); i++) {
                expression = new AndExpression(expression, conditions.get(i));
            }
        }
        return expression;
    }

    /**
     * Get the sorted table name from the expression
     * @param expression the expression to be sorted
     * @param adjustedList the adjusted list
     * @return the sorted table name
     */
    public static List<String> getSortedTableNameFromExpression(Expression expression, List<String> adjustedList) {
        List<String> res = extractTableNamesFromExpression(expression);
        if (adjustedList.indexOf(res.get(0)) > adjustedList.indexOf(res.get(1))) {
            String temp = res.get(0);
            res.set(0, res.get(1));
            res.set(1, temp);
        }
        return res;
    }
}
