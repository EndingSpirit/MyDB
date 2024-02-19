package ed.inf.adbs.lightdb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Catlog {

    private static Catlog instance = null;
    private Map<String, List<String>> tableSchemas;

    private Catlog() {
        tableSchemas = new HashMap<>();
    }

    public static Catlog getInstance() {
        if (instance == null) {
            instance = new Catlog();
        }
        return instance;
    }

    public void addTableSchema(String tableName, List<String> schema) {
        tableSchemas.put(tableName, schema);
    }

    public List<String> getTableSchema(String tableName) {
        return tableSchemas.getOrDefault(tableName, null);
    }

    public void loadSchema(String schemaFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                List<String> columns = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    columns.add(parts[i]); // 直接添加列名到列表
                }
                tableSchemas.put(tableName, columns); // 存储表名及其列名的列表
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from file: " + schemaFilePath, e);
        }
    }
}
