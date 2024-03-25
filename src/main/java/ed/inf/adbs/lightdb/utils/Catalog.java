package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Catalog {

    private static Catalog instance = null;
    private final Map<String, List<String>> tableSchemas;
    private final Map<String, String> tableAliases;
    private final Map<String, Map<String, String>> columnAliases;
    private final Map<String, List<String>> accumulatedSchema;

    private Catalog() {
        this.tableSchemas = new HashMap<>();
        this.tableAliases = new HashMap<>();
        this.columnAliases = new HashMap<>();
        this.accumulatedSchema = new HashMap<>();
    }
    public static Catalog getInstance() {
        if (instance == null) {
            instance = new Catalog();
        }
        return instance;
    }



    public void loadSchema(String schemaFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                List<String> columns = new ArrayList<>(Arrays.asList(parts).subList(1, parts.length));
                tableSchemas.put(tableName, columns);
                // 初始化列别名映射
                columnAliases.put(tableName, new HashMap<>());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from file: " + schemaFilePath, e);
        }
    }


    public String resolveTableName(String name) {
        String resolved = tableAliases.get(name);
        if (resolved != null) {
            return resolved;
        }

        String[] parts = name.split("\\s+");
        if (parts.length > 1) {
            resolved = tableAliases.get(parts[parts.length - 1]);
            if (resolved != null) {
                return resolved;
            }
        }
        return name;
    }

    public List<String> getTableSchema(String tableName) {
        // Resolve alias before getting the schema
        String actualTableName = resolveTableName(tableName.split(" ")[0].trim());
        return tableSchemas.get(actualTableName);
    }

    public void setTableAlias(String alias, String actualTableName) {
        tableAliases.put(alias, actualTableName);
        // 同时复制列别名映射，以支持对别名表使用列别名
        if (columnAliases.containsKey(actualTableName)) {
            columnAliases.put(alias, new HashMap<>(columnAliases.get(actualTableName)));
        }
    }
    public String getTableNameByAlias(String alias) {
        for (Map.Entry<String, String> entry : tableAliases.entrySet()) {
            String value = entry.getValue();
            String key = entry.getKey();
            if (key.equals(alias)) {
                return value;
            }
        }
        return null;
    }
    public void setAccumulatedSchema(String tableName, List<String> schema) {
        accumulatedSchema.put(tableName, schema);
    }

    public List<String> getAccumulatedSchema(String tableName) {
        return accumulatedSchema.get(tableName);
    }

    public List<String> getSchemasFromPlain(PlainSelect plainSelect) {
        List<String> schemas = new ArrayList<>();
        String tableName = plainSelect.getFromItem().toString();
        addTableSchema(schemas, tableName);

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                tableName = join.getRightItem().toString();
                addTableSchema(schemas, tableName);
            }
        }

        return schemas;
    }

    private void addTableSchema(List<String> schemas, String tableName) {
        String resolvedTableName = resolveTableName(tableName.split(" ")[0]);
        List<String> tableSchema = getTableSchema(resolvedTableName);
        if (Config.getInstance().isUseAliases()) {
            String alias = tableName.contains(" ") ? tableName.split(" ")[1] : null;
            if (alias != null) {
                for (String column : tableSchema) {
                    if (column.contains(".")){
                        schemas.add(alias + "." + column.split("\\.")[1].trim());
                    }else{
                        schemas.add(alias + "." + column);
                    }
                }
            } else {
                schemas.addAll(tableSchema);
            }
        } else {
            schemas.addAll(tableSchema);
        }
    }


    public void setColumnAlias(String tableName, String columnAlias, String actualColumnName) {
        tableName = resolveTableName(tableName);
        if (!columnAliases.containsKey(tableName)) {
            columnAliases.put(tableName, new HashMap<>());
        }
        columnAliases.get(tableName).put(columnAlias, actualColumnName);
    }

    public String resolveColumnName(String tableName, String columnName) {
        tableName = resolveTableName(tableName);
        if (columnAliases.containsKey(tableName) && columnAliases.get(tableName).containsKey(columnName)) {
            return columnAliases.get(tableName).get(columnName);
        }
        return columnName; // Return the original name if no alias is found
    }

    public String getTableAliases(String table) {
        return tableAliases.get(table);
    }
}
