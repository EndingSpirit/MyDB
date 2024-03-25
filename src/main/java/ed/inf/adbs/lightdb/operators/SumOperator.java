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

public class SumOperator extends Operator {
    private final Operator child;
    private final List<Expression> sumExpressions;
    private final List<Column> groupByAttributes;
    private final PlainSelect plainSelect;
    private final List<Tuple> resultTuples = new ArrayList<>();

    public final List<String> groupBySchema;
    private boolean hasProcessed = false;
    private int currentIndex = 0;

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

    private void processAllTuples() {
        Map<String, List<Tuple>> groups = new HashMap<>();
        List<String> schema = Catalog.getInstance().getSchemasFromPlain(plainSelect);
        Tuple tuple;

        // Step 1: 分组收集元组
        while ((tuple = child.getNextTuple()) != null) {
            String key = buildGroupByKey(tuple, schema);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }

        // Step 2: 对每个分组进行SUM计算
        for (Map.Entry<String, List<Tuple>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<Tuple> tuplesInGroup = entry.getValue();
            Integer[] sumResults = new Integer[sumExpressions.size()];

            for (Tuple t : tuplesInGroup) {
                calculate(t, sumResults, schema);
            }

            // 将分组键和SUM结果转换为一个新的Tuple
            List<Integer> resultFields = new ArrayList<>();
            if (!groupKey.isEmpty()) {
                for (String keyValue : groupKey.split(",")) {
                    resultFields.add(Integer.parseInt(keyValue));
                }
            }
            resultFields.addAll(Arrays.asList(sumResults));
            resultTuples.add(new Tuple(resultFields));
        }
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
