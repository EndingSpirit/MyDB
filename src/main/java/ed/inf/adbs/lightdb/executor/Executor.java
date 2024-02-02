package ed.inf.adbs.lightdb.executor;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.FileWriterUtil;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Executor {

    public Executor() {
    }

    public static void parse() throws Exception {
        try {
            String filename = Config.getInstance().getInputFilePath();
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));

            if (statement instanceof Select) {
                // 处理SELECT语句
                handleSelect((Select) statement);
            } else {
                // 处理其他类型的语句或抛出异常
                throw new UnsupportedOperationException("Unsupported SQL statement");
            }
        } catch (JSQLParserException e) {
            throw new RuntimeException("Error parsing SQL statement: " + e.getMessage());
        }
    }


    private static void handleSelect(Select selectStatement) {
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

        if (plainSelect.getWhere() == null) {
            scan(plainSelect);

        } else {
            // 处理其他类型的 SELECT 查询
        }
    }
    private static void scan(PlainSelect plainSelect){
        Table table = (Table) plainSelect.getFromItem();
        String tableName = table.getName();
        try {
            ScanOperator scanOperator = new ScanOperator(tableName);
            Tuple tuple;
            List<Tuple> tuples = new ArrayList<>();
            while ((tuple = scanOperator.getNextTuple()) != null) {
                    tuples.add(tuple);
            }
            FileWriterUtil.writeTuplesToFile(tuples);
        } catch (IOException e) {
            System.err.println("Error during scan operation: " + e.getMessage());
        }
    }
}
