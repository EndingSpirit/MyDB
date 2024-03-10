package ed.inf.adbs.lightdb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Catalog {

    private static Catalog instance = null;
    private final Map<String, List<String>> tableSchemas;

    private Catalog() {
        tableSchemas = new HashMap<>();
    }

    public static Catalog getInstance() {
        if (instance == null) {
            instance = new Catalog();
        }
        return instance;
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
                List<String> columns = new ArrayList<>(Arrays.asList(parts).subList(1, parts.length));
                tableSchemas.put(tableName, columns); // 存储表名及其列名的列表
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from file: " + schemaFilePath, e);
        }
    }
}
