package ed.inf.adbs.lightdb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Catalog {

    private static Catalog instance = null;
    private final Map<String, List<String>> tableSchemas;
    // This map is for alias -> actual table name resolution
    private final Map<String, String> aliasesToTableNames = new HashMap<>();


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
        // Resolve alias before getting the schema
        String actualTableName = resolveTableName(tableName);
        return tableSchemas.get(actualTableName);
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

    public String resolveTableName(String name) {
        String resolved = aliasesToTableNames.get(name);
        if (resolved != null) {
            return resolved;
        }

        String[] parts = name.split("\\s+");
        if (parts.length > 1) {
            resolved = aliasesToTableNames.get(parts[parts.length - 1]);
            if (resolved != null) {
                return resolved;
            }
        }

        return name;
    }


    public void setTableAlias(String alias, String tableName) {
        // The problem seems to be here; ensure tableName is correctly parsed
        aliasesToTableNames.put(alias, tableName.trim()); // Trimming to remove any leading/trailing spaces
    }

    public void addTableSchema(String tableName, List<String> columns) {
        tableSchemas.put(tableName, columns);
    }
}
