package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.utils.DatabaseSchema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanOperator extends Operator {

    private String tableName;
    private BufferedReader reader;
    private Map<String, Integer> columnToIndexMap; // 新增

    private String databaseDir = Config.getInstance().getDbPath();

    public ScanOperator(String tableName) throws IOException {
        this.tableName = tableName;
        // 获取数据库 schema 信息
        DatabaseSchema schema = DatabaseSchema.getInstance();
        this.columnToIndexMap = schema.getTableSchema(tableName);
        if (columnToIndexMap == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist in the schema.");
        }
        this.reader = new BufferedReader(new FileReader(databaseDir + "/data/" + tableName + ".csv"));
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                String[] values = line.split(",");
                List<Integer> fields = new ArrayList<>();
                for (String value : values) {
                    fields.add(Integer.parseInt(value.trim())); // 将字符串值转换为整数
                }
                // 注意：创建 Tuple 时不再需要传递 columnToIndexMap，因为 Tuple 类已经假定所有字段都是整型
                return new Tuple(fields,columnToIndexMap); // 更新创建 Tuple 的方式
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
            reader = new BufferedReader(new FileReader(databaseDir + "/data/" + tableName + ".csv"));
        } catch (IOException e) {
            throw new RuntimeException("Error resetting ScanOperator: " + e.getMessage());
        }
    }
}

