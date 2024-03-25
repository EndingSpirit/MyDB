package ed.inf.adbs.lightdb.operators;

import java.util.*;

import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.FunctionExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * SumOperator is used to calculate the sum of the columns in the select clause
 * and group by the columns in the group by clause
 */
public class SumOperator extends Operator {
    private final Operator child;
    private final List<Expression> sumExpressions;
    private final List<Column> groupByAttributes;
    private final PlainSelect plainSelect;
    private final List<Tuple> resultTuples = new ArrayList<>();

    public final List<String> groupBySchema;
    private boolean hasProcessed = false;
    private int currentIndex = 0;

    /**
     * Constructor for SumOperator
     * @param child The child operator
     * @param sumExpressions The sum expressions
     * @param groupByAttributes The group by attributes
     * @param plainSelect The select clause
     */
    public SumOperator(Operator child, List<Expression> sumExpressions, List<Column> groupByAttributes, PlainSelect plainSelect) {
        this.child = child;
        this.sumExpressions = sumExpressions;
        this.groupByAttributes = groupByAttributes;
        this.plainSelect = plainSelect;
        this.groupBySchema = new ArrayList<>();
    }

    @Override
    public Tuple getNextTuple() {
        if (!hasProcessed) {
            processAllTuples();
            hasProcessed = true;
        }
        if (currentIndex < resultTuples.size()) {
            return resultTuples.get(currentIndex++);
        }
        return null;
    }

    /**
     * Process all the tuples from the child operator
     */
    private void processAllTuples() {
        Map<String, List<Tuple>> groups = new HashMap<>();
        List<String> schema = Catalog.getInstance().getSchemasFromPlain(plainSelect);
        Tuple tuple;

        // Group collects tuples with the same group key
        while ((tuple = child.getNextTuple()) != null) {
            String key = buildGroupByKey(tuple, schema);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }

        // Calculate SUM for each group
        for (Map.Entry<String, List<Tuple>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<Tuple> tuplesInGroup = entry.getValue();
            Integer[] sumResults = new Integer[sumExpressions.size()];

            for (Tuple t : tuplesInGroup) {
                calculate(t, sumResults, schema);
            }

            // Converts the group key and SUM result to a new Tuple
            List<Integer> resultFields = new ArrayList<>();
            if (!groupKey.isEmpty()) {
                for (String keyValue : groupKey.split(",")) {
                    resultFields.add(Integer.parseInt(keyValue));
                }
            }
            resultFields.addAll(Arrays.asList(sumResults));
            resultTuples.add(new Tuple(resultFields));
        }

        // Add the group by schema to the schema
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expression = item.getExpression();

            if (expression instanceof Column) {
                if (Config.getInstance().isUseAliases()) {
                    String columnName = ((Column) expression).getFullyQualifiedName();
                    groupBySchema.add(columnName);
                } else {
                    String columnName = ((Column) expression).getColumnName();
                    groupBySchema.add(columnName);
                }
            } else if (expression instanceof Function) {
                String functionName = expression.toString();
                groupBySchema.add(functionName);
            } else {
                groupBySchema.add(expression.toString());
            }
        }

    }

    /**
     * Calculate the sum of the expression
     * @param tuple The tuple to be evaluated
     * @param sumResults The sum results
     * @param schema The schema of the table
     */
    private void calculate(Tuple tuple, Integer[] sumResults, List<String> schema) {
        for (int i = 0; i < sumExpressions.size(); i++) {
            Function sumFunction = (Function) sumExpressions.get(i);
            if (sumFunction.getParameters() != null) {
                ExpressionList<?> functionBody = sumFunction.getParameters();
                for (Expression expr : functionBody) {
                    Integer value = new FunctionExpressionDeParser(expr, schema).evaluate(tuple);

                    if (sumResults[i] == null) {
                        sumResults[i] = value;
                    } else {
                        sumResults[i] += value;
                    }
                }
            }
        }
    }


    /**
     * Build the group key by the group by attributes
     * @param tuple The tuple to be evaluated
     * @param schema The schema of the table
     * @return The group key
     */
    private String buildGroupByKey(Tuple tuple, List<String> schema) {
        if (groupByAttributes.isEmpty()) {
            return "";
        }
        StringBuilder keyBuilder = new StringBuilder();
        for (Column attr : groupByAttributes) {
            String ColumnName;
            if (Config.getInstance().isUseAliases()) {
                ColumnName = attr.getFullyQualifiedName();
            } else {
                ColumnName = attr.getColumnName();
            }
            int index = schema.indexOf(ColumnName);
            if (index != -1) {
                if (keyBuilder.length() > 0) keyBuilder.append(",");
                keyBuilder.append(tuple.getField(index));
            }
        }
        return keyBuilder.toString();
    }


    @Override
    public void reset() {
        child.reset();
        hasProcessed = false;
        currentIndex = 0;
        resultTuples.clear();
    }
}
