package ed.inf.adbs.lightdb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseSchema {
    private static DatabaseSchema instance;
    private Map<String, Map<String, Integer>> schemaInfo;

    private DatabaseSchema() {
        schemaInfo = new HashMap<>();
    }

    public static synchronized DatabaseSchema getInstance() {
        if (instance == null) {
            instance = new DatabaseSchema();
        }
        return instance;
    }

    public void loadSchema(String schemaFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                Map<String, Integer> columns = new HashMap<>();
                for (int i = 1; i < parts.length; i++) {
                    columns.put(parts[i], i - 1); // Subtract 1 to convert to 0-based index
                }
                schemaInfo.put(tableName, columns);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from file: " + schemaFilePath, e);
        }
    }

    public Map<String, Integer> getTableSchema(String tableName) {
        return schemaInfo.get(tableName);
    }
}
