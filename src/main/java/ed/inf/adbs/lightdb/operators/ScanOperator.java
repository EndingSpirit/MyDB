package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScanOperator extends Operator {

    private String tableName;
    private BufferedReader reader;
    private String currentLine;

    private String databaseDir = Config.getInstance().getDbPath();

    public ScanOperator(String tableName) throws IOException {
        this.tableName = tableName;
        // 假设数据文件与表名同名，位于某个固定的目录下
        this.reader = new BufferedReader(new FileReader(databaseDir+"/data/" + tableName + ".csv"));
    }

    @Override
    public Tuple getNextTuple() {
        try {
            if ((currentLine = reader.readLine()) != null) {
                // 假设数据文件中的每一行是用逗号分隔的值
                String[] values = currentLine.split("\n");
                List<Object> fields = new ArrayList<>(Arrays.asList(values));
                return new Tuple(fields);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file: " + e.getMessage());
        }
    }

    @Override
    public void reset() {
        try {
            reader.close();
            reader = new BufferedReader(new FileReader(databaseDir+"/data/" + tableName + ".csv"));
        } catch (IOException e) {
            throw new RuntimeException("Error resetting ScanOperator: " + e.getMessage());
        }
    }
}

