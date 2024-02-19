package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.utils.Config;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.FileReader;
import java.io.IOException;


/**
 * The purpose of this class is to execute a query.
 * Include reading statement from the query file
 * identify the type of the statement and execute planner
 */
public class Executor {

    /**
     * Execute the query
     *  @throws Exception
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
        Operator finalOperator = Planner.constructQueryPlan(plainSelect);

        finalOperator.dump();
    }

}
