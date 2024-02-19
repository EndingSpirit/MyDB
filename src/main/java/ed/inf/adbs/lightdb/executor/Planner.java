package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectionOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;

public class Planner {
    // Constructs the operator chain based on the SQL query
    public static Operator constructQueryPlan(PlainSelect plainSelect) throws IOException {
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan = new ScanOperator(tableName);

        if (plainSelect.getWhere() != null) {
            queryPlan = new SelectOperator(queryPlan, plainSelect.getWhere(), tableName);
        }

        if (plainSelect.getSelectItems() != null) {
            queryPlan = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
        }

        // 可以添加更多操作符

        return queryPlan;
    }
}
