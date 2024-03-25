package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * Catalog class is used to store the schema and alias information of the tables
 */
public class Catalog {

    private static Catalog instance = null;
    private final Map<String, List<String>> tableSchemas;
    private final Map<String, String> tableAlias;
    private final Map<String, Map<String, String>> columnAlias;
    private final Map<String, List<String>> accumulatedSchema;


    /**
     * Constructor of the Catalog
     */
    private Catalog() {
        this.tableSchemas = new HashMap<>();
        this.tableAlias = new HashMap<>();
        this.columnAlias = new HashMap<>();
        this.accumulatedSchema = new HashMap<>();
    }

    /**
     * Get the instance of the Catalog
     * @return the instance of the Catalog
     */
    public static Catalog getInstance() {
        if (instance == null) {
            instance = new Catalog();
        }
        return instance;
    }

    /**
     * Load the schema from the schema file
     * @param schemaFilePath the path of the schema file
     * @throws RuntimeException if failed to load schema from file
     */
    public void loadSchema(String schemaFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                String tableName = parts[0];
                List<String> columns = new ArrayList<>(Arrays.asList(parts).subList(1, parts.length));
                tableSchemas.put(tableName, columns);
                // 初始化列别名映射
                columnAlias.put(tableName, new HashMap<>());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema from file: " + schemaFilePath, e);
        }
    }


    /**
    * Resolve the table name to the actual table name
     * @param tableName the table name to be resolved
     * @return the resolved table name
     *                  It could be the actual table name or the alias name
    **/
    public String resolveTableName(String tableName) {
        String resolved = tableAlias.get(tableName);
        if (resolved != null) {
            return resolved;
        }

        String[] parts = tableName.split("\\s+");
        if (parts.length > 1) {
            resolved = tableAlias.get(parts[parts.length - 1]);
            if (resolved != null) {
                return resolved;
            }
        }
        return tableName;
    }

    /**
     * get the schema of the table
     * @param tableName the table name to get the schema
     * @return the schema of the table
     *                  It could be the actual table name or the alias name
     **/
    public List<String> getTableSchema(String tableName) {
        // Resolve alias before getting the schema
        String actualTableName = resolveTableName(tableName.split(" ")[0].trim());
        return tableSchemas.get(actualTableName);
    }

    /**
     * set the schema of the table
     * @param alias the alias of the table
     * @param actualTableName the actual table name
     **/
    public void setTableAlias(String alias, String actualTableName) {
        tableAlias.put(alias, actualTableName);
        // Also copy the column alias map to support the use of column aliases for the alias table
        if (columnAlias.containsKey(actualTableName)) {
            columnAlias.put(alias, new HashMap<>(columnAlias.get(actualTableName)));
        }
    }

    /**
     * set the column alias of the table
     * @param table the table name
     * @param schema the schema of the table
     *               Add the column alias to the schema
     **/
    public static void resolveAliasSchema(String table, List<String> schema) {
        if (Config.getInstance().isUseAliases()) {
            String alias = table.contains(" ") ? table.split(" ")[1] : null;
            if (alias != null && !schema.get(0).contains(".")) {
                schema.replaceAll(s -> alias + "." + s);
            }
        }
    }

    /**
     * This method is used to set AccumulatedSchema, which is for manage the schema more easily for JoinOperator
     * @param tableName the table name
     * @param schema the schema of the table
     */
    public void setAccumulatedSchema(String tableName, List<String> schema) {
        accumulatedSchema.put(tableName, schema);
    }


    /**
     * This method gives another way to get the schema of the table, that is from the PlainSelect
     * It is for projection operator to get the target schema
     * @param plainSelect the PlainSelect object
     * **/
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

    /**
     * This method is to support getSchemasFromPlain method to get the schema of the table
     * @param schemas the schema list
     * @param tableName the table name
     * **/
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

}
