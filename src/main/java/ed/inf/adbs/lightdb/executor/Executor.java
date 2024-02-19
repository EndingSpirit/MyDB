package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectionOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.Catlog;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * The purpose of this class is to execute a query.
 * Include reading statement from the query file
 * and constructing the operator chain based on the SQL query
 */
public class Executor {

    /**
     * Execute the query
     *
     * @throws Exception
     */
    public static void execute() throws Exception {
        try {
            String filename = Config.getInstance().getInputFilePath();
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            System.out.println("Executing query: " + statement);
            if (statement instanceof Select) {
                handleSelect((Select) statement);
            } else {
                throw new UnsupportedOperationException("Unsupported SQL statement");
            }
        } catch (JSQLParserException e) {
            throw new RuntimeException("Error parsing SQL statement: " + e.getMessage());
        }
    }

    private static void handleSelect(Select selectStatement) throws IOException {
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        Operator finalOperator = constructQueryPlan(plainSelect);

        finalOperator.dump();
    }

    // Constructs the operator chain based on the SQL query
    private static Operator constructQueryPlan(PlainSelect plainSelect) throws IOException {
        String tableName = plainSelect.getFromItem().toString();
        Operator queryPlan = new ScanOperator(tableName);

        if (plainSelect.getWhere() != null) {
            queryPlan = new SelectOperator(queryPlan, plainSelect.getWhere(), tableName);
        }

//        if (plainSelect.getSelectItems() != null) {
//            ProjectionOperator projectOperator = new ProjectionOperator(queryPlan, plainSelect.getSelectItems(), tableName);
//        }

        // 可以添加更多操作符

        return queryPlan;
    }

}
