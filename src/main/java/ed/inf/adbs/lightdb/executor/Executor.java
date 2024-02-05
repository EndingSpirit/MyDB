package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectionOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.FileWriterUtil;
import ed.inf.adbs.lightdb.utils.DatabaseCatalog;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Executor {

    public static void execute() throws Exception {
        try {
            String filename = Config.getInstance().getInputFilePath();
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));

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
        Operator finalOperator = constructOperatorChain(plainSelect);

        List<Tuple> tuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = finalOperator.getNextTuple()) != null) {
            tuples.add(tuple);
        }
        FileWriterUtil.writeTuplesToFile(tuples);
    }

    // Constructs the operator chain based on the SQL query
    private static Operator constructOperatorChain(PlainSelect plainSelect) throws IOException {
        Operator operator = new ScanOperator(plainSelect.getFromItem().toString());
        Map<String, Integer> schema = DatabaseCatalog.getInstance().getTableSchema(plainSelect.getFromItem().toString());

        if (plainSelect.getWhere() != null) {
            operator = new SelectOperator(operator, plainSelect.getWhere(), plainSelect.getFromItem().toString());
        }

        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        if (selectItems != null && !selectItems.isEmpty()) {
            boolean projectAllColumns = selectItems.stream().anyMatch(item -> item.toString().equals("*"));
            if (!projectAllColumns) {
                operator = new ProjectionOperator(operator, selectItems); // tableName should be defined based on your context
            }
        }

        // Add more operators here as needed, e.g., Join, OrderBy, Distinct, GroupBy

        return operator;
    }

}
