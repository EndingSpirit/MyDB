package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class SortOperator extends Operator {

    private final PlainSelect plainSelect;
    private final Operator child;
    private final List<Tuple> sortedTuples;
    private int currentTupleIndex = 0;
    private boolean isSorted = false; // 布尔标志以确保排序只执行一次
    private final List<String> schema;

    public SortOperator(PlainSelect plainSelect, Operator child) {
        this.plainSelect = plainSelect;
        this.child = child;
        this.schema = Catalog.getInstance().getSchemasFromPlain(plainSelect); // Schema should include aliases if present
        this.sortedTuples = new ArrayList<>();
    }

    @Override
    public Tuple getNextTuple() {
        if (!isSorted) {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                sortedTuples.add(tuple);
            }
            if (plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty()) {
                sortedTuples.sort((t1, t2) -> {
                    List<SelectItem<?>> selectItemList = this.plainSelect.getSelectItems();
                    List<String> selectItems = new ArrayList<>();

                    if (selectItemList.get(0).toString().equals("*")) {
                        selectItems.addAll(schema);
                    } else {
                        selectItemList.forEach(selectItem -> selectItems.add(Config.getInstance().isUseAliases() ? selectItem.toString() : selectItem.toString().split("\\.")[1]));
                    }

                    for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                        Expression expr = orderByElement.getExpression();
                        if (expr instanceof Column) {
                            Column column = (Column) expr;
                            String columnName = Config.getInstance().isUseAliases() ? column.toString() : column.getColumnName();
                            int columnIndex = selectItems.indexOf(columnName);
                            if (columnIndex == -1) {
                                throw new RuntimeException("Column " + columnName + " not found in schema.");
                            }
                            Comparable<Integer> value1 = t1.getFields().get(columnIndex);
                            Integer value2 = t2.getFields().get(columnIndex);
                            int comparison = value1.compareTo(value2);
                            if (comparison != 0) {
                                return orderByElement.isAsc() ? comparison : -comparison;
                            }
                        }
                    }
                    return 0;
                });
            }
            isSorted = true; // 标记为已排序
        }

        if (currentTupleIndex < sortedTuples.size()) {
            return sortedTuples.get(currentTupleIndex++);
        }
        return null; // 没有更多元组可返回
    }

    @Override
    public void reset() {
        currentTupleIndex = 0;
        isSorted = false; // 重置排序状态
        sortedTuples.clear(); // 清空已排序的元组列表
    }
}

